use super::control::ControlSignal;
use super::coordinated_execution::{apply_completed_coordinated_run, spawn_coordinated_trigger};
use super::engine::Scheduler;
use super::overlap::{OverlapAction, dispatch_trigger, take_queued_if_idle};
use super::trigger::{PendingTrigger, TriggerDecision, next_trigger};
use crate::ExecutionGuard;
use crate::coordinated_store::{
    CoordinatedLeaseConfig, CoordinatedPendingTrigger, CoordinatedRuntimeState,
    CoordinatedStateStore,
};
use crate::error::SchedulerError;
use crate::model::{Job, JobState, SchedulerReport, TerminalStatePolicy};
use crate::observer::{SchedulerEvent, SchedulerStopReason, StateLoadSource};
use chrono::Utc;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::task::JoinSet;

pub(super) async fn run_coordinated_scheduler<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    job: Job<D>,
    store: &Arc<C>,
    lease_config: CoordinatedLeaseConfig,
) -> Result<SchedulerReport, SchedulerError>
where
    S: crate::StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    let mut runtime =
        load_or_initialize_coordinated_state(scheduler, store.as_ref(), &job, true).await?;
    let mut state = runtime.state.clone();
    let mut history = VecDeque::new();
    let mut last_skip_reason = None;
    let mut active = JoinSet::new();
    let mut active_count = 0usize;
    let mut queued_trigger: Option<PendingTrigger> = None;
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
            runtime = load_or_initialize_coordinated_state(scheduler, store.as_ref(), &job, false)
                .await?;
            state = runtime.state.clone();

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

                let claim = store
                    .claim_trigger(
                        &job.job_id,
                        &job.execution_resource_id,
                        runtime.revision,
                        CoordinatedPendingTrigger {
                            scheduled_at: trigger.scheduled_at,
                            catch_up: trigger.catch_up,
                            trigger_count: trigger.trigger_count,
                        },
                        &runtime.state,
                        lease_config,
                    )
                    .await
                    .map_err(|error| {
                        let kind = C::classify_guard_error(&error);
                        SchedulerError::execution_guard(error, kind)
                    })?;
                if let Some(claim) = claim {
                    scheduler.emit(SchedulerEvent::ExecutionGuardAcquired {
                        job_id: claim.lease.job_id.clone(),
                        resource_id: claim.lease.resource_id.clone(),
                        scope: claim.lease.scope,
                        lease_key: claim.lease.lease_key.clone(),
                        scheduled_at: claim.lease.scheduled_at,
                        catch_up: trigger.catch_up,
                        trigger_count: trigger.trigger_count,
                    });
                    spawn_coordinated_trigger(
                        scheduler,
                        store.clone(),
                        lease_config,
                        &mut active,
                        &job,
                        claim,
                    )
                    .await;
                    active_count += 1;
                }
                continue;
            }

            if let Some(claim) = store
                .reclaim_inflight(&job.job_id, &job.execution_resource_id, lease_config)
                .await
                .map_err(|error| {
                    let kind = C::classify_guard_error(&error);
                    SchedulerError::execution_guard(error, kind)
                })?
            {
                scheduler.emit(SchedulerEvent::ExecutionGuardAcquired {
                    job_id: claim.lease.job_id.clone(),
                    resource_id: claim.lease.resource_id.clone(),
                    scope: claim.lease.scope,
                    lease_key: claim.lease.lease_key.clone(),
                    scheduled_at: claim.lease.scheduled_at,
                    catch_up: claim.trigger.catch_up,
                    trigger_count: claim.trigger.trigger_count,
                });
                spawn_coordinated_trigger(
                    scheduler,
                    store.clone(),
                    lease_config,
                    &mut active,
                    &job,
                    claim,
                )
                .await;
                active_count += 1;
                continue;
            }

            let now = Utc::now();
            if scheduler.should_wait_for_active_replay(&job, active_count) {
            } else {
                let mut candidate_state = runtime.state.clone();
                match next_trigger(&job, &mut candidate_state, now, scheduler.config.timezone)? {
                    TriggerDecision::Idle => {
                        state = candidate_state;
                    }
                    TriggerDecision::StateAdvanced => {
                        let saved = store
                            .save_state(&job.job_id, runtime.revision, &candidate_state)
                            .await
                            .map_err(|error| {
                                let kind = C::classify_store_error(&error);
                                SchedulerError::store(error, kind)
                            })?;
                        if saved {
                            state = candidate_state;
                        }
                    }
                    TriggerDecision::Trigger(trigger) => {
                        let now = Utc::now();
                        if let Some(reason) = job.skip_reason_at(now, scheduler.config.timezone) {
                            let saved = store
                                .save_state(&job.job_id, runtime.revision, &candidate_state)
                                .await
                                .map_err(|error| {
                                    let kind = C::classify_store_error(&error);
                                    SchedulerError::store(error, kind)
                                })?;
                            if saved {
                                state = candidate_state;
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
                                let claim = store
                                    .claim_trigger(
                                        &job.job_id,
                                        &job.execution_resource_id,
                                        runtime.revision,
                                        CoordinatedPendingTrigger {
                                            scheduled_at: trigger.scheduled_at,
                                            catch_up: trigger.catch_up,
                                            trigger_count: trigger.trigger_count,
                                        },
                                        &candidate_state,
                                        lease_config,
                                    )
                                    .await
                                    .map_err(|error| {
                                        let kind = C::classify_guard_error(&error);
                                        SchedulerError::execution_guard(error, kind)
                                    })?;
                                if let Some(claim) = claim {
                                    state = claim.state.state.clone();
                                    scheduler.emit(SchedulerEvent::TriggerEmitted {
                                        job_id: job.job_id.clone(),
                                        scheduled_at: trigger.scheduled_at,
                                        catch_up: trigger.catch_up,
                                        trigger_count: trigger.trigger_count,
                                    });
                                    scheduler.emit(SchedulerEvent::ExecutionGuardAcquired {
                                        job_id: claim.lease.job_id.clone(),
                                        resource_id: claim.lease.resource_id.clone(),
                                        scope: claim.lease.scope,
                                        lease_key: claim.lease.lease_key.clone(),
                                        scheduled_at: claim.lease.scheduled_at,
                                        catch_up: trigger.catch_up,
                                        trigger_count: trigger.trigger_count,
                                    });
                                    spawn_coordinated_trigger(
                                        scheduler,
                                        store.clone(),
                                        lease_config,
                                        &mut active,
                                        &job,
                                        claim,
                                    )
                                    .await;
                                    active_count += 1;
                                    continue;
                                }
                            }
                            OverlapAction::QueueUpdated | OverlapAction::Dropped => {
                                let saved = store
                                    .save_state(&job.job_id, runtime.revision, &candidate_state)
                                    .await
                                    .map_err(|error| {
                                        let kind = C::classify_store_error(&error);
                                        SchedulerError::store(error, kind)
                                    })?;
                                if saved {
                                    state = candidate_state;
                                }
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
                store.delete(&job.job_id).await.map_err(|error| {
                    let kind = C::classify_store_error(&error);
                    SchedulerError::store(error, kind)
                })?;
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
                    apply_completed_coordinated_run(scheduler, store.as_ref(), &mut state, &mut history, completed).await?;
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
        apply_completed_coordinated_run(
            scheduler,
            store.as_ref(),
            &mut state,
            &mut history,
            completed,
        )
        .await?;
    }

    Ok(SchedulerReport {
        job_id: job.job_id.clone(),
        state,
        history: history.into_iter().collect(),
        last_skip_reason,
    })
}

async fn load_or_initialize_coordinated_state<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    store: &C,
    job: &Job<D>,
    emit_load_event: bool,
) -> Result<CoordinatedRuntimeState, SchedulerError>
where
    S: crate::StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    let initial_state = JobState::new(
        job.job_id.clone(),
        crate::scheduler::trigger_math::initial_next_run_at(job, scheduler.config.timezone)?,
    );
    let mut runtime = store
        .load_or_initialize(&job.job_id, initial_state)
        .await
        .map_err(|error| {
            let kind = C::classify_store_error(&error);
            SchedulerError::store(error, kind)
        })?;

    if scheduler.should_repair_interval_state(job, &runtime.state) {
        let previous_next_run_at = runtime.state.next_run_at;
        runtime.state.next_run_at =
            crate::scheduler::trigger_math::initial_next_run_at(job, scheduler.config.timezone)?;
        let saved = store
            .save_state(&job.job_id, runtime.revision, &runtime.state)
            .await
            .map_err(|error| {
                let kind = C::classify_store_error(&error);
                SchedulerError::store(error, kind)
            })?;
        if saved {
            runtime.revision += 1;
            scheduler.emit(SchedulerEvent::StateRepaired {
                job_id: job.job_id.clone(),
                trigger_count: runtime.state.trigger_count,
                previous_next_run_at,
                repaired_next_run_at: runtime.state.next_run_at,
            });
            if emit_load_event {
                scheduler.emit(SchedulerEvent::StateLoaded {
                    job_id: job.job_id.clone(),
                    trigger_count: runtime.state.trigger_count,
                    next_run_at: runtime.state.next_run_at,
                    source: StateLoadSource::Repaired,
                });
            }
        }
    } else if emit_load_event {
        scheduler.emit(SchedulerEvent::StateLoaded {
            job_id: job.job_id.clone(),
            trigger_count: runtime.state.trigger_count,
            next_run_at: runtime.state.next_run_at,
            source: if runtime.revision == 0 {
                StateLoadSource::New
            } else {
                StateLoadSource::Restored
            },
        });
    }

    Ok(runtime)
}
