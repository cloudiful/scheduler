use crate::model::{
    JobState, RunContext, RunRecord, RunStatus, TaskHandler, TriggeredTaskContext, push_history,
};
use crate::observer::{SchedulerEvent, SchedulerObserver};
use crate::scheduler::control::{ControlSignal, StopSignal};
use crate::scheduler::runtime_events::execution_guard_released;
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

pub(crate) fn apply_completed_run(
    state: &mut JobState,
    history: &mut VecDeque<RunRecord>,
    history_limit: usize,
    completed: CompletedRun,
) -> (RunRecord, u32) {
    let trigger_count = completed.trigger_count;
    let record = completed.apply_to(state, history, history_limit);
    (record, trigger_count)
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
        let task_future = task(TriggeredTaskContext {
            run: RunContext {
                job_id: job_id.clone(),
                scheduled_at: trigger.scheduled_at,
                catch_up: trigger.catch_up,
                timezone,
            },
            deps,
            source: trigger.source,
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
            observer.on_event(&execution_guard_released(
                &lease,
                trigger.catch_up,
                trigger.trigger_count,
            ));
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

#[cfg(test)]
mod tests {
    use super::{CompletedRun, apply_completed_run};
    use crate::model::{JobState, RunRecord, RunStatus};
    use chrono::Utc;
    use std::collections::VecDeque;

    #[test]
    fn apply_completed_run_updates_state_and_enforces_history_limit() {
        let t0 = Utc::now();
        let mut state = JobState::new("job", Some(t0));
        let mut history = VecDeque::from([RunRecord {
            scheduled_at: t0,
            started_at: t0,
            finished_at: t0,
            catch_up: false,
            status: RunStatus::Success,
            error: None,
        }]);
        let t1 = t0 + chrono::TimeDelta::seconds(1);

        let (record, trigger_count) = apply_completed_run(
            &mut state,
            &mut history,
            1,
            CompletedRun {
                record: RunRecord {
                    scheduled_at: t1,
                    started_at: t1,
                    finished_at: t1,
                    catch_up: true,
                    status: RunStatus::Failed,
                    error: Some("boom".to_string()),
                },
                trigger_count: 7,
            },
        );

        assert_eq!(trigger_count, 7);
        assert_eq!(record.scheduled_at, t1);
        assert_eq!(state.last_run_at, Some(t1));
        assert_eq!(state.last_error.as_deref(), Some("boom"));
        assert_eq!(history.len(), 1);
        assert_eq!(history.front().expect("history item").scheduled_at, t1);
    }
}
