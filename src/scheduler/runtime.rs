use super::control::{CommandDisposition, SchedulerMode, StopSignal};
use super::overlap::{OverlapAction, dispatch_trigger, take_queued_if_idle};
use super::trigger::{PendingTrigger, TriggerDecision, next_trigger};
use crate::error::SchedulerError;
use crate::model::{Job, JobState, RunRecord, SchedulerReport, TriggerSource};
use crate::observer::{PauseScope, SchedulerEvent, SchedulerStopReason};
use crate::scheduler::trigger_math::{advance_state_for_manual_trigger, next_run_is_in_future};
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

    fn is_paused(&self, runtime: &Self::Runtime) -> bool;

    fn pause_scope(&self) -> PauseScope;

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

    fn pause<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        job: &'a Job<D>,
        runtime: &'a mut Self::Runtime,
    ) -> impl std::future::Future<Output = Result<bool, SchedulerError>> + Send + 'a;

    fn resume<'a>(
        &'a self,
        scheduler: &'a super::engine::Scheduler<S, G, C>,
        job: &'a Job<D>,
        runtime: &'a mut Self::Runtime,
    ) -> impl std::future::Future<Output = Result<bool, SchedulerError>> + Send + 'a;
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
    let mut initial_control = *scheduler.control.borrow();
    initial_control.stop_signal = None;
    let _ = scheduler.control.send(initial_control);
    let mut control_rx = scheduler.control.subscribe();
    let mut last_seen_mode_command = 0u64;

    loop {
        runtime = backend.refresh_runtime(scheduler, &job, runtime).await?;
        let control = *control_rx.borrow();

        if let Some(stop_signal) = control.stop_signal
            && active_count == 0
        {
            scheduler.emit(SchedulerEvent::SchedulerStopped {
                job_id: job.job_id.clone(),
                trigger_count: backend.state(&runtime).trigger_count,
                reason: match stop_signal {
                    StopSignal::Cancel => SchedulerStopReason::Cancelled,
                    StopSignal::Shutdown => SchedulerStopReason::Shutdown,
                },
            });
            break;
        }

        if control.mode_command_seq != last_seen_mode_command {
            let changed = match control.command_disposition {
                CommandDisposition::Apply => match control.desired_mode {
                    SchedulerMode::Running => backend.resume(scheduler, &job, &mut runtime).await?,
                    SchedulerMode::Paused => backend.pause(scheduler, &job, &mut runtime).await?,
                },
                CommandDisposition::ObserveOnly { changed } => changed,
            };
            scheduler
                .applied_modes
                .lock()
                .unwrap()
                .insert(job.job_id.clone(), control.desired_mode);
            last_seen_mode_command = control.mode_command_seq;
            if changed {
                match control.desired_mode {
                    SchedulerMode::Paused => scheduler.emit(SchedulerEvent::SchedulerPaused {
                        job_id: job.job_id.clone(),
                        trigger_count: backend.state(&runtime).trigger_count,
                        scope: backend.pause_scope(),
                    }),
                    SchedulerMode::Running => scheduler.emit(SchedulerEvent::SchedulerResumed {
                        job_id: job.job_id.clone(),
                        trigger_count: backend.state(&runtime).trigger_count,
                        scope: backend.pause_scope(),
                    }),
                }
            }
        }

        if control.stop_signal.is_none() && !backend.is_paused(&runtime) {
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

            let pending_manual_request = scheduler
                .manual_triggers
                .lock()
                .unwrap()
                .remove(&job.job_id);
            if let Some(request) = pending_manual_request {
                let mut candidate_state = backend.state(&runtime).clone();
                let trigger = PendingTrigger {
                    scheduled_at: request.requested_at,
                    catch_up: false,
                    trigger_count: advance_state_for_manual_trigger(&job, &mut candidate_state),
                    source: TriggerSource::Manual,
                };

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
        if !backend.is_paused(&runtime)
            && next_run_at.is_none()
            && active_count == 0
            && queued_trigger.is_none()
            && !scheduler.manual_triggers.lock().unwrap().contains_key(&job.job_id)
        {
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
            _ = scheduler.sleep_until_next(next_run_at), if control_rx.borrow().stop_signal.is_none() && matches!(control_rx.borrow().desired_mode, SchedulerMode::Running) && !backend.is_paused(&runtime) && queued_trigger.is_none() && !scheduler.manual_triggers.lock().unwrap().contains_key(&job.job_id) && next_run_is_in_future(next_run_at) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)), if backend.is_paused(&runtime) => {}
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
