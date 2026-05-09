use super::control::StopSignal;
use super::engine::Scheduler;
use crate::coordinated_store::{CoordinatedClaim, CoordinatedLeaseConfig, CoordinatedStateStore};
use crate::error::SchedulerError;
use crate::model::{Job, JobState, RunRecord};
use crate::observer::SchedulerEvent;
use crate::{ExecutionGuard, ExecutionGuardRenewal, ExecutionLease};
use chrono::Utc;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::task::JoinSet;
use super::execution::{CompletedRun, renewal_schedule};

#[derive(Debug)]
pub(super) struct CoordinatedCompletedRun {
    pub(super) completed: CompletedRun,
    pub(super) lease: ExecutionLease,
    pub(super) revision: u64,
}

pub(super) async fn spawn_coordinated_trigger<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    store: Arc<C>,
    lease_config: CoordinatedLeaseConfig,
    active: &mut JoinSet<CoordinatedCompletedRun>,
    job: &Job<D>,
    claim: CoordinatedClaim,
) where
    S: crate::StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    let observer = scheduler.observer.clone();
    let control = scheduler.control.clone();
    let task = job.task.clone();
    let deps = job.deps.clone();
    let timezone = scheduler.config.timezone;
    let job_id = job.job_id.clone();
    active.spawn(async move {
        let started_at = Utc::now();
        let run_future = task(crate::TaskContext {
            run: crate::RunContext {
                job_id,
                scheduled_at: claim.trigger.scheduled_at,
                catch_up: claim.trigger.catch_up,
                timezone,
            },
            deps,
        });
        tokio::pin!(run_future);
        let mut renewal_count = 0u32;
        let mut failed_renewal_count = 0u32;
        let mut lost_reported = false;
        let mut renewal = renewal_schedule(Some(lease_config.renew_interval));

        let result = loop {
            if let Some(ticker) = renewal.as_mut() {
                let mut disable_renewal = false;
                let outcome = tokio::select! {
                    result = &mut run_future => Some(result),
                    _ = ticker.tick() => {
                        match store.renew(&claim.lease, lease_config).await {
                            Ok(ExecutionGuardRenewal::Renewed) => {
                                renewal_count += 1;
                                observer.on_event(&SchedulerEvent::ExecutionGuardRenewed {
                                    job_id: claim.lease.job_id.clone(),
                                    resource_id: claim.lease.resource_id.clone(),
                                    scope: claim.lease.scope,
                                    lease_key: claim.lease.lease_key.clone(),
                                    scheduled_at: claim.lease.scheduled_at,
                                    catch_up: claim.trigger.catch_up,
                                    trigger_count: claim.trigger.trigger_count,
                                    renewal_count,
                                });
                            }
                            Ok(ExecutionGuardRenewal::Lost) => {
                                if !lost_reported {
                                    observer.on_event(&SchedulerEvent::ExecutionGuardLost {
                                        job_id: claim.lease.job_id.clone(),
                                        resource_id: claim.lease.resource_id.clone(),
                                        scope: claim.lease.scope,
                                        lease_key: claim.lease.lease_key.clone(),
                                        scheduled_at: claim.lease.scheduled_at,
                                        catch_up: claim.trigger.catch_up,
                                        trigger_count: claim.trigger.trigger_count,
                                        renewal_count,
                                        failed_renewal_count,
                                    });
                                    let mut next = *control.borrow();
                                    next.stop_signal = Some(StopSignal::Shutdown);
                                    let _ = control.send(next);
                                    lost_reported = true;
                                }
                                disable_renewal = true;
                            }
                            Err(error) => {
                                failed_renewal_count += 1;
                                observer.on_event(&SchedulerEvent::ExecutionGuardRenewFailed {
                                    job_id: claim.lease.job_id.clone(),
                                    resource_id: claim.lease.resource_id.clone(),
                                    scope: claim.lease.scope,
                                    lease_key: claim.lease.lease_key.clone(),
                                    scheduled_at: claim.lease.scheduled_at,
                                    catch_up: claim.trigger.catch_up,
                                    trigger_count: claim.trigger.trigger_count,
                                    renewal_count,
                                    failed_renewal_count,
                                    error: error.to_string(),
                                });
                                if !lost_reported {
                                    observer.on_event(&SchedulerEvent::ExecutionGuardLost {
                                        job_id: claim.lease.job_id.clone(),
                                        resource_id: claim.lease.resource_id.clone(),
                                        scope: claim.lease.scope,
                                        lease_key: claim.lease.lease_key.clone(),
                                        scheduled_at: claim.lease.scheduled_at,
                                        catch_up: claim.trigger.catch_up,
                                        trigger_count: claim.trigger.trigger_count,
                                        renewal_count,
                                        failed_renewal_count,
                                    });
                                    let mut next = *control.borrow();
                                    next.stop_signal = Some(StopSignal::Shutdown);
                                    let _ = control.send(next);
                                    lost_reported = true;
                                }
                                disable_renewal = true;
                            }
                        }
                        None
                    }
                };

                if disable_renewal {
                    renewal = None;
                }

                if let Some(result) = outcome {
                    break result;
                }
            } else {
                break run_future.await;
            }
        };
        let finished_at = Utc::now();
        let (status, error) = match result {
            Ok(()) => (crate::RunStatus::Success, None),
            Err(message) => (crate::RunStatus::Failed, Some(message)),
        };
        CoordinatedCompletedRun {
            completed: CompletedRun {
                record: crate::RunRecord {
                    scheduled_at: claim.trigger.scheduled_at,
                    started_at,
                    finished_at,
                    catch_up: claim.trigger.catch_up,
                    status,
                    error,
                },
                trigger_count: claim.trigger.trigger_count,
            },
            lease: claim.lease,
            revision: claim.state.revision,
        }
    });
}

pub(super) async fn apply_completed_coordinated_run<S, G, C>(
    scheduler: &Scheduler<S, G, C>,
    store: &C,
    state: &mut JobState,
    history: &mut VecDeque<RunRecord>,
    completed: CoordinatedCompletedRun,
) -> Result<(), SchedulerError>
where
    S: crate::StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: CoordinatedStateStore + Send + Sync + 'static,
{
    let trigger_count = completed.completed.trigger_count;
    let record = completed
        .completed
        .apply_to(state, history, scheduler.config.history_limit);
    let saved = store
        .complete(&state.job_id, completed.revision, &completed.lease, state)
        .await
        .map_err(|error| {
            let kind = C::classify_store_error(&error);
            SchedulerError::store(error, kind)
        })?;
    if saved {
        scheduler.emit(SchedulerEvent::ExecutionGuardReleased {
            job_id: completed.lease.job_id.clone(),
            resource_id: completed.lease.resource_id.clone(),
            scope: completed.lease.scope,
            lease_key: completed.lease.lease_key.clone(),
            scheduled_at: completed.lease.scheduled_at,
            catch_up: record.catch_up,
            trigger_count,
        });
    }
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
