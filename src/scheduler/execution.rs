use crate::model::{
    JobState, RunContext, RunRecord, RunStatus, TaskContext, TaskHandler, push_history,
};
use crate::observer::{SchedulerEvent, SchedulerObserver};
use crate::scheduler::control::{ControlSignal, StopSignal};
use crate::scheduler::trigger::PendingTrigger;
use crate::{ExecutionGuard, ExecutionGuardRenewal, ExecutionLease};
use chrono::Utc;
use chrono_tz::Tz;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant, interval_at};

#[derive(Debug)]
pub(crate) struct CompletedRun {
    pub(crate) record: RunRecord,
    pub(crate) trigger_count: u32,
}

impl CompletedRun {
    pub(crate) fn apply_to(
        self,
        state: &mut JobState,
        history: &mut VecDeque<RunRecord>,
        history_limit: usize,
    ) -> RunRecord {
        state.last_run_at = Some(self.record.started_at);
        match self.record.status {
            RunStatus::Success => {
                state.last_success_at = Some(self.record.finished_at);
                state.last_error = None;
            }
            RunStatus::Failed => {
                state.last_error = self.record.error.clone();
            }
        }

        push_history(history, self.record.clone(), history_limit);
        self.record
    }
}

pub(crate) fn spawn_legacy_trigger<D, G>(
    active: &mut JoinSet<CompletedRun>,
    task: TaskHandler<D>,
    deps: Arc<D>,
    job_id: String,
    timezone: Tz,
    trigger: PendingTrigger,
    guard: Arc<G>,
    observer: Arc<dyn SchedulerObserver>,
    control: watch::Sender<ControlSignal>,
    lease: ExecutionLease,
) where
    G: ExecutionGuard + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    active.spawn(async move {
        let started_at = Utc::now();
        let mut renewal_count = 0u32;
        let mut failed_renewal_count = 0u32;
        let task_future = task(TaskContext {
            run: RunContext {
                job_id: job_id.clone(),
                scheduled_at: trigger.scheduled_at,
                catch_up: trigger.catch_up,
                timezone,
            },
            deps,
        });
        tokio::pin!(task_future);
        let renew_every = guard.renew_interval(&lease);
        let mut renewal = renewal_schedule(renew_every);
        let mut lost_reported = false;

        let result = loop {
            if let Some(ticker) = renewal.as_mut() {
                let mut stop_renewal = false;
                let outcome = tokio::select! {
                    result = &mut task_future => Some(result),
                    _ = ticker.tick() => {
                        let renew_result = guard.renew(&lease).await;
                        match renew_result {
                            Ok(ExecutionGuardRenewal::Renewed) => {
                                renewal_count += 1;
                                observer.on_event(&SchedulerEvent::ExecutionGuardRenewed {
                                    job_id: job_id.clone(),
                                    resource_id: lease.resource_id.clone(),
                                    scope: lease.scope,
                                    lease_key: lease.lease_key.clone(),
                                    scheduled_at: lease.scheduled_at,
                                    catch_up: trigger.catch_up,
                                    trigger_count: trigger.trigger_count,
                                    renewal_count,
                                });
                            }
                            Ok(ExecutionGuardRenewal::Lost) => {
                                if !lost_reported {
                                    observer.on_event(&SchedulerEvent::ExecutionGuardLost {
                                        job_id: job_id.clone(),
                                        resource_id: lease.resource_id.clone(),
                                        scope: lease.scope,
                                        lease_key: lease.lease_key.clone(),
                                        scheduled_at: lease.scheduled_at,
                                        catch_up: trigger.catch_up,
                                        trigger_count: trigger.trigger_count,
                                        renewal_count,
                                        failed_renewal_count,
                                    });
                                    let mut next = *control.borrow();
                                    next.stop_signal = Some(StopSignal::Shutdown);
                                    let _ = control.send(next);
                                    lost_reported = true;
                                }
                                stop_renewal = true;
                            }
                            Err(error) => {
                                failed_renewal_count += 1;
                                observer.on_event(&SchedulerEvent::ExecutionGuardRenewFailed {
                                    job_id: job_id.clone(),
                                    resource_id: lease.resource_id.clone(),
                                    scope: lease.scope,
                                    lease_key: lease.lease_key.clone(),
                                    scheduled_at: lease.scheduled_at,
                                    catch_up: trigger.catch_up,
                                    trigger_count: trigger.trigger_count,
                                    renewal_count,
                                    failed_renewal_count,
                                    error: error.to_string(),
                                });
                                if !lost_reported {
                                    observer.on_event(&SchedulerEvent::ExecutionGuardLost {
                                        job_id: job_id.clone(),
                                        resource_id: lease.resource_id.clone(),
                                        scope: lease.scope,
                                        lease_key: lease.lease_key.clone(),
                                        scheduled_at: lease.scheduled_at,
                                        catch_up: trigger.catch_up,
                                        trigger_count: trigger.trigger_count,
                                        renewal_count,
                                        failed_renewal_count,
                                    });
                                    let mut next = *control.borrow();
                                    next.stop_signal = Some(StopSignal::Shutdown);
                                    let _ = control.send(next);
                                    lost_reported = true;
                                }
                                stop_renewal = true;
                            }
                        }
                        None
                    }
                };

                if stop_renewal {
                    renewal = None;
                }

                if let Some(result) = outcome {
                    break result;
                }
            } else {
                break task_future.await;
            }
        };
        let finished_at = Utc::now();

        let (status, error) = match result {
            Ok(()) => (RunStatus::Success, None),
            Err(message) => (RunStatus::Failed, Some(message)),
        };

        if let Err(error) = guard.release(&lease).await {
            observer.on_event(&SchedulerEvent::ExecutionGuardReleaseFailed {
                job_id: job_id.clone(),
                resource_id: lease.resource_id.clone(),
                scope: lease.scope,
                lease_key: lease.lease_key.clone(),
                scheduled_at: lease.scheduled_at,
                catch_up: trigger.catch_up,
                trigger_count: trigger.trigger_count,
                error: error.to_string(),
            });
        } else {
            observer.on_event(&SchedulerEvent::ExecutionGuardReleased {
                job_id: job_id.clone(),
                resource_id: lease.resource_id.clone(),
                scope: lease.scope,
                lease_key: lease.lease_key.clone(),
                scheduled_at: lease.scheduled_at,
                catch_up: trigger.catch_up,
                trigger_count: trigger.trigger_count,
            });
        }

        CompletedRun {
            record: RunRecord {
                scheduled_at: trigger.scheduled_at,
                started_at,
                finished_at,
                catch_up: trigger.catch_up,
                status,
                error,
            },
            trigger_count: trigger.trigger_count,
        }
    });
}

pub(crate) fn renewal_schedule(renew_interval: Option<Duration>) -> Option<tokio::time::Interval> {
    renew_interval.map(|duration| interval_at(Instant::now() + duration, duration))
}
