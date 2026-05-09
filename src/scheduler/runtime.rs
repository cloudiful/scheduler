use super::control::ControlSignal;
use super::overlap::{OverlapAction, dispatch_trigger, take_queued_if_idle};
use super::trigger::{PendingTrigger, TriggerDecision, next_trigger};
use crate::error::SchedulerError;
use crate::model::{Job, JobState, RunRecord, SchedulerReport};
use crate::observer::{SchedulerEvent, SchedulerStopReason};
use crate::scheduler::trigger_math::next_run_is_in_future;
use chrono::Utc;
use std::collections::VecDeque;
use tokio::task::JoinSet;

pub(super) trait SchedulerRuntimeBackend<S, G, C, D>
where
    S: crate::StateStore + Send + Sync + 'static,
    G: crate::ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Runtime: Send + 'static;
    type Completed: Send + 'static;

    fn initialize<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        job: &'a Job<D>,
    ) -> impl std::future::Future<Output = Result<Self::Runtime, SchedulerError>> + Send + 'a;

    fn refresh_runtime<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        job: &'a Job<D>,
        runtime: Self::Runtime,
    ) -> impl std::future::Future<Output = Result<Self::Runtime, SchedulerError>> + Send + 'a;

    fn state<'a>(&self, runtime: &'a Self::Runtime) -> &'a JobState;

    fn save_state<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        job: &'a Job<D>,
        runtime: &'a mut Self::Runtime,
        state: &'a JobState,
    ) -> impl std::future::Future<Output = Result<bool, SchedulerError>> + Send + 'a;

    fn handle_queued_trigger<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        job: &'a Job<D>,
        runtime: &'a mut Self::Runtime,
        trigger: PendingTrigger,
        active: &'a mut JoinSet<Self::Completed>,
    ) -> impl std::future::Future<Output = Result<bool, SchedulerError>> + Send + 'a;

    fn try_reclaim_inflight<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        job: &'a Job<D>,
        runtime: &'a mut Self::Runtime,
        active: &'a mut JoinSet<Self::Completed>,
    ) -> impl std::future::Future<Output = Result<bool, SchedulerError>> + Send + 'a;

    fn handle_due_trigger<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        job: &'a Job<D>,
        runtime: &'a mut Self::Runtime,
        candidate_state: JobState,
        trigger: PendingTrigger,
        overlap_action: OverlapAction,
        active: &'a mut JoinSet<Self::Completed>,
    ) -> impl std::future::Future<Output = Result<bool, SchedulerError>> + Send + 'a;

    fn apply_completed<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        runtime: &'a mut Self::Runtime,
        history: &'a mut VecDeque<RunRecord>,
        completed: Self::Completed,
    ) -> impl std::future::Future<Output = Result<(), SchedulerError>> + Send + 'a;

    fn delete_terminal_state<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        job: &'a Job<D>,
        runtime: &'a mut Self::Runtime,
    ) -> impl std::future::Future<Output = Result<(), SchedulerError>> + Send + 'a;
}

pub(super) async fn run_scheduler<S, G, C, D, B>(
    scheduler: &super::engine::Scheduler<S, G, C>,
    job: Job<D>,
    backend: &B,
) -> Result<SchedulerReport, SchedulerError>
where
    S: crate::StateStore + Send + Sync + 'static,
    G: crate::ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
    B: SchedulerRuntimeBackend<S, G, C, D>,
{
    let mut runtime = backend.initialize(scheduler, &job).await?;
    let mut history = VecDeque::new();
    let mut last_skip_reason = None;
    let mut active = JoinSet::new();
    let mut active_count = 0usize;
    let mut queued_trigger = None;
    let _ = scheduler.control.send(ControlSignal::Running);
    let mut control_rx = scheduler.control.subscribe();

    loop {
        if matches!(
            *control_rx.borrow(),
            ControlSignal::Cancel | ControlSignal::Shutdown
        ) && active_count == 0
        {
            scheduler.emit(SchedulerEvent::SchedulerStopped {
                job_id: job.job_id.clone(),
                trigger_count: backend.state(&runtime).trigger_count,
                reason: match *control_rx.borrow() {
                    ControlSignal::Cancel => SchedulerStopReason::Cancelled,
                    ControlSignal::Shutdown => SchedulerStopReason::Shutdown,
                    ControlSignal::Running => SchedulerStopReason::ChannelClosed,
                },
            });
            break;
        }

        if matches!(*control_rx.borrow(), ControlSignal::Running) {
            runtime = backend.refresh_runtime(scheduler, &job, runtime).await?;

            if let Some(trigger) = take_queued_if_idle(active_count, &mut queued_trigger) {
                let now = Utc::now();
                if let Some(reason) = job.skip_reason_at(now, scheduler.config.timezone) {
                    last_skip_reason = Some(reason);
                    scheduler.emit(SchedulerEvent::RunSkipped {
                        job_id: job.job_id.clone(),
                        scheduled_at: trigger.scheduled_at,
                        catch_up: trigger.catch_up,
                        trigger_count: trigger.trigger_count,
                        reason,
                    });
                    continue;
                }

                if backend
                    .handle_queued_trigger(scheduler, &job, &mut runtime, trigger, &mut active)
                    .await?
                {
                    active_count += 1;
                }
                continue;
            }

            if backend
                .try_reclaim_inflight(scheduler, &job, &mut runtime, &mut active)
                .await?
            {
                active_count += 1;
                continue;
            }

            let now = Utc::now();
            if !scheduler.should_wait_for_active_replay(&job, active_count) {
                let mut candidate_state = backend.state(&runtime).clone();
                match next_trigger(&job, &mut candidate_state, now, scheduler.config.timezone)? {
                    TriggerDecision::Idle => {}
                    TriggerDecision::StateAdvanced => {
                        if backend
                            .save_state(scheduler, &job, &mut runtime, &candidate_state)
                            .await?
                        {
                            continue;
                        }
                    }
                    TriggerDecision::Trigger(trigger) => {
                        let now = Utc::now();
                        if let Some(reason) = job.skip_reason_at(now, scheduler.config.timezone) {
                            if backend
                                .save_state(scheduler, &job, &mut runtime, &candidate_state)
                                .await?
                            {
                                last_skip_reason = Some(reason);
                                scheduler.emit(SchedulerEvent::RunSkipped {
                                    job_id: job.job_id.clone(),
                                    scheduled_at: trigger.scheduled_at,
                                    catch_up: trigger.catch_up,
                                    trigger_count: trigger.trigger_count,
                                    reason,
                                });
                            }
                            continue;
                        }

                        match dispatch_trigger(
                            job.overlap_policy,
                            active_count,
                            &mut queued_trigger,
                            trigger,
                        ) {
                            OverlapAction::Spawn(trigger) => {
                                if backend
                                    .handle_due_trigger(
                                        scheduler,
                                        &job,
                                        &mut runtime,
                                        candidate_state,
                                        trigger,
                                        OverlapAction::Spawn(trigger),
                                        &mut active,
                                    )
                                    .await?
                                {
                                    active_count += 1;
                                }
                                continue;
                            }
                            OverlapAction::QueueUpdated | OverlapAction::Dropped => {
                                let _ = backend
                                    .handle_due_trigger(
                                        scheduler,
                                        &job,
                                        &mut runtime,
                                        candidate_state,
                                        trigger,
                                        OverlapAction::QueueUpdated,
                                        &mut active,
                                    )
                                    .await?;
                                continue;
                            }
                        }
                    }
                }
            }
        }

        let next_run_at = backend.state(&runtime).next_run_at;
        if next_run_at.is_none() && active_count == 0 && queued_trigger.is_none() {
            if matches!(
                scheduler.config.terminal_state_policy,
                crate::model::TerminalStatePolicy::Delete
            ) {
                backend
                    .delete_terminal_state(scheduler, &job, &mut runtime)
                    .await?;
                scheduler.emit(SchedulerEvent::TerminalStateDeleted {
                    job_id: job.job_id.clone(),
                    trigger_count: backend.state(&runtime).trigger_count,
                });
            }
            scheduler.emit(SchedulerEvent::SchedulerStopped {
                job_id: job.job_id.clone(),
                trigger_count: backend.state(&runtime).trigger_count,
                reason: SchedulerStopReason::Terminal,
            });
            break;
        }

        tokio::select! {
            maybe_result = active.join_next(), if active_count > 0 => {
                if let Some(result) = maybe_result {
                    active_count -= 1;
                    let completed = result.map_err(SchedulerError::task_join)?;
                    backend.apply_completed(
                        scheduler,
                        &mut runtime,
                        &mut history,
                        completed,
                    ).await?;
                }
            }
            changed = control_rx.changed() => {
                if changed.is_err() {
                    scheduler.emit(SchedulerEvent::SchedulerStopped {
                        job_id: job.job_id.clone(),
                        trigger_count: backend.state(&runtime).trigger_count,
                        reason: SchedulerStopReason::ChannelClosed,
                    });
                    break;
                }
            }
            _ = scheduler.sleep_until_next(next_run_at), if matches!(*control_rx.borrow(), ControlSignal::Running) && queued_trigger.is_none() && next_run_is_in_future(next_run_at) => {}
        }
    }

    while let Some(result) = active.join_next().await {
        let completed = result.map_err(SchedulerError::task_join)?;
        backend
            .apply_completed(scheduler, &mut runtime, &mut history, completed)
            .await?;
    }

    Ok(SchedulerReport {
        job_id: job.job_id.clone(),
        state: backend.state(&runtime).clone(),
        history: history.into_iter().collect(),
        last_skip_reason,
    })
}
