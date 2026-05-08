use super::control::ControlSignal;
use super::engine::Scheduler;
use super::execution::CompletedRun;
use super::overlap::{OverlapAction, dispatch_trigger, take_queued_if_idle};
use super::trigger::{PendingTrigger, TriggerDecision, next_trigger};
use crate::error::SchedulerError;
use crate::model::{Job, JobState, RunRecord, SchedulerReport, TerminalStatePolicy};
use crate::observer::{SchedulerEvent, SchedulerStopReason, StateLoadSource};
use crate::store::StateStore;
use crate::{ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardScope, ExecutionSlot};
use chrono::Utc;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::task::JoinSet;

pub(super) async fn run_legacy_scheduler<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    job: Job<D>,
    store: &Arc<S>,
    guard: &Arc<G>,
) -> Result<SchedulerReport, SchedulerError>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    let (mut state, state_is_new) = load_or_initialize_legacy_state(scheduler, store, &job).await?;
    let mut history = VecDeque::new();
    let mut last_skip_reason = None;
    let mut active = JoinSet::new();
    let mut active_count = 0usize;
    let mut queued_trigger = None;
    let _ = scheduler.control.send(ControlSignal::Running);
    let mut control_rx = scheduler.control.subscribe();
    if state_is_new {
        scheduler.persist_state_to_legacy(store, &state).await?;
    }

    loop {
        if matches!(
            *control_rx.borrow(),
            ControlSignal::Cancel | ControlSignal::Shutdown
        ) && active_count == 0
        {
            scheduler.emit(SchedulerEvent::SchedulerStopped {
                job_id: job.job_id.clone(),
                trigger_count: state.trigger_count,
                reason: match *control_rx.borrow() {
                    ControlSignal::Cancel => SchedulerStopReason::Cancelled,
                    ControlSignal::Shutdown => SchedulerStopReason::Shutdown,
                    ControlSignal::Running => SchedulerStopReason::ChannelClosed,
                },
            });
            break;
        }

        if matches!(*control_rx.borrow(), ControlSignal::Running) {
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

                if try_spawn_legacy_trigger(scheduler, guard, &job, &mut active, trigger).await? {
                    active_count += 1;
                }
                continue;
            }

            let now = Utc::now();
            if scheduler.should_wait_for_active_replay(&job, active_count) {
            } else {
                match next_trigger(&job, &mut state, now, scheduler.config.timezone)? {
                    TriggerDecision::Idle => {}
                    TriggerDecision::StateAdvanced => {
                        scheduler.persist_state_to_legacy(store, &state).await?;
                    }
                    TriggerDecision::Trigger(trigger) => {
                        scheduler.persist_state_to_legacy(store, &state).await?;
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

                        scheduler.emit(SchedulerEvent::TriggerEmitted {
                            job_id: job.job_id.clone(),
                            scheduled_at: trigger.scheduled_at,
                            catch_up: trigger.catch_up,
                            trigger_count: trigger.trigger_count,
                        });
                        match dispatch_trigger(
                            job.overlap_policy,
                            active_count,
                            &mut queued_trigger,
                            trigger,
                        ) {
                            OverlapAction::Spawn(trigger) => {
                                if try_spawn_legacy_trigger(
                                    scheduler,
                                    guard,
                                    &job,
                                    &mut active,
                                    trigger,
                                )
                                .await?
                                {
                                    active_count += 1;
                                }
                                continue;
                            }
                            OverlapAction::QueueUpdated | OverlapAction::Dropped => {
                                continue;
                            }
                        }
                    }
                }
            }
        }

        if state.next_run_at.is_none() && active_count == 0 && queued_trigger.is_none() {
            if matches!(
                scheduler.config.terminal_state_policy,
                TerminalStatePolicy::Delete
            ) {
                scheduler
                    .delete_state_from_legacy(store, &job.job_id)
                    .await?;
                scheduler.emit(SchedulerEvent::TerminalStateDeleted {
                    job_id: job.job_id.clone(),
                    trigger_count: state.trigger_count,
                });
            }
            scheduler.emit(SchedulerEvent::SchedulerStopped {
                job_id: job.job_id.clone(),
                trigger_count: state.trigger_count,
                reason: SchedulerStopReason::Terminal,
            });
            break;
        }

        tokio::select! {
            maybe_result = active.join_next(), if active_count > 0 => {
                if let Some(result) = maybe_result {
                    active_count -= 1;
                    let completed = result.map_err(SchedulerError::task_join)?;
                    apply_completed_legacy_run(scheduler, store, &mut state, &mut history, completed).await?;
                }
            }
            changed = control_rx.changed() => {
                if changed.is_err() {
                    scheduler.emit(SchedulerEvent::SchedulerStopped {
                        job_id: job.job_id.clone(),
                        trigger_count: state.trigger_count,
                        reason: SchedulerStopReason::ChannelClosed,
                    });
                    break;
                }
            }
            _ = scheduler.sleep_until_next(state.next_run_at), if matches!(*control_rx.borrow(), ControlSignal::Running) && queued_trigger.is_none() && crate::scheduler::trigger_math::next_run_is_in_future(state.next_run_at) => {}
        }
    }

    while let Some(result) = active.join_next().await {
        let completed = result.map_err(SchedulerError::task_join)?;
        apply_completed_legacy_run(scheduler, store, &mut state, &mut history, completed).await?;
    }

    Ok(SchedulerReport {
        job_id: job.job_id.clone(),
        state,
        history: history.into_iter().collect(),
        last_skip_reason,
    })
}

async fn load_or_initialize_legacy_state<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    store: &S,
    job: &Job<D>,
) -> Result<(JobState, bool), SchedulerError>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    match scheduler.load_state_from_legacy(store, &job.job_id).await? {
        Some(state) => restore_legacy_state(scheduler, store, job, state).await,
        None => {
            let state = JobState::new(
                job.job_id.clone(),
                crate::scheduler::trigger_math::initial_next_run_at(
                    job,
                    scheduler.config.timezone,
                )?,
            );
            scheduler.emit(SchedulerEvent::StateLoaded {
                job_id: job.job_id.clone(),
                trigger_count: state.trigger_count,
                next_run_at: state.next_run_at,
                source: StateLoadSource::New,
            });
            Ok((state, true))
        }
    }
}

async fn restore_legacy_state<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    store: &S,
    job: &Job<D>,
    mut state: JobState,
) -> Result<(JobState, bool), SchedulerError>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    if scheduler.should_repair_interval_state(job, &state) {
        let previous_next_run_at = state.next_run_at;
        state.next_run_at =
            crate::scheduler::trigger_math::initial_next_run_at(job, scheduler.config.timezone)?;
        scheduler.persist_state_to_legacy(store, &state).await?;
        scheduler.emit(SchedulerEvent::StateRepaired {
            job_id: job.job_id.clone(),
            trigger_count: state.trigger_count,
            previous_next_run_at,
            repaired_next_run_at: state.next_run_at,
        });
        scheduler.emit(SchedulerEvent::StateLoaded {
            job_id: job.job_id.clone(),
            trigger_count: state.trigger_count,
            next_run_at: state.next_run_at,
            source: StateLoadSource::Repaired,
        });
    } else {
        scheduler.emit(SchedulerEvent::StateLoaded {
            job_id: job.job_id.clone(),
            trigger_count: state.trigger_count,
            next_run_at: state.next_run_at,
            source: StateLoadSource::Restored,
        });
    }

    Ok((state, false))
}

async fn apply_completed_legacy_run<S, G, C>(
    scheduler: &Scheduler<S, G, C>,
    store: &S,
    state: &mut JobState,
    history: &mut VecDeque<RunRecord>,
    completed: CompletedRun,
) -> Result<(), SchedulerError>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
{
    let trigger_count = completed.trigger_count;
    let record = completed.apply_to(state, history, scheduler.config.history_limit);
    scheduler.persist_state_to_legacy(store, state).await?;
    scheduler.emit(SchedulerEvent::RunCompleted {
        job_id: state.job_id.clone(),
        scheduled_at: record.scheduled_at,
        catch_up: record.catch_up,
        trigger_count,
        status: record.status,
        error: record.error,
    });
    Ok(())
}

async fn try_spawn_legacy_trigger<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    guard: &Arc<G>,
    job: &Job<D>,
    active: &mut JoinSet<CompletedRun>,
    trigger: PendingTrigger,
) -> Result<bool, SchedulerError>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    let slot = match job.guard_scope {
        ExecutionGuardScope::Occurrence => ExecutionSlot::for_occurrence(
            job.job_id.clone(),
            job.execution_resource_id.clone(),
            trigger.scheduled_at,
        ),
        ExecutionGuardScope::Resource => {
            ExecutionSlot::for_resource(job.job_id.clone(), job.execution_resource_id.clone())
        }
    };
    let acquired = guard.acquire(slot).await.map_err(|error| {
        let kind = G::classify_error(&error);
        SchedulerError::execution_guard(error, kind)
    })?;

    let ExecutionGuardAcquire::Acquired(lease) = acquired else {
        scheduler.emit(SchedulerEvent::ExecutionGuardContended {
            job_id: job.job_id.clone(),
            resource_id: job.execution_resource_id.clone(),
            scope: job.guard_scope,
            scheduled_at: Some(trigger.scheduled_at),
            catch_up: trigger.catch_up,
            trigger_count: trigger.trigger_count,
        });
        return Ok(false);
    };

    scheduler.emit(SchedulerEvent::ExecutionGuardAcquired {
        job_id: job.job_id.clone(),
        resource_id: lease.resource_id.clone(),
        scope: lease.scope,
        lease_key: lease.lease_key.clone(),
        scheduled_at: lease.scheduled_at,
        catch_up: trigger.catch_up,
        trigger_count: trigger.trigger_count,
    });

    crate::scheduler::execution::spawn_trigger(
        active,
        job.task.clone(),
        job.deps.clone(),
        job.job_id.clone(),
        scheduler.config.timezone,
        trigger,
        guard.clone(),
        scheduler.observer.clone(),
        scheduler.control.clone(),
        lease,
    );
    Ok(true)
}
