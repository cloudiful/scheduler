use chrono::{Datelike, Utc};
use scheduler::{
    CoordinatedClaim, CoordinatedLeaseConfig, CoordinatedPendingTrigger, CoordinatedRuntimeState,
    CoordinatedStateStore, ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardRenewal,
    ExecutionGuardScope, ExecutionLease, ExecutionSlot, GuardedRunResult, GuardedRunner, Job,
    JobTimeWindow, OverlapPolicy, RunSkipReason, Schedule, Scheduler, SchedulerConfig,
    SchedulerEvent, SchedulerObserver, Task,
};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
struct InMemoryScopeGuard {
    state: Arc<Mutex<GuardState>>,
}

#[derive(Default)]
struct GuardState {
    resource_locks: HashSet<String>,
    occurrence_locks: HashMap<String, HashSet<String>>,
}

impl ExecutionGuard for InMemoryScopeGuard {
    type Error = Infallible;

    async fn acquire(&self, slot: ExecutionSlot) -> Result<ExecutionGuardAcquire, Self::Error> {
        let mut state = self.state.lock().unwrap();
        let resource_locked = state.resource_locks.contains(&slot.resource_id);

        let acquired = match slot.scope {
            ExecutionGuardScope::Occurrence => {
                if resource_locked {
                    false
                } else {
                    state
                        .occurrence_locks
                        .entry(slot.resource_id.clone())
                        .or_default()
                        .insert(slot.scheduled_at.expect("occurrence slot").to_rfc3339())
                }
            }
            ExecutionGuardScope::Resource => {
                let has_occurrences = state
                    .occurrence_locks
                    .get(&slot.resource_id)
                    .map(|entries| !entries.is_empty())
                    .unwrap_or(false);
                if resource_locked || has_occurrences {
                    false
                } else {
                    state.resource_locks.insert(slot.resource_id.clone())
                }
            }
        };

        Ok(if acquired {
            ExecutionGuardAcquire::Acquired(ExecutionLease::new(
                slot.job_id,
                slot.resource_id,
                slot.scope,
                slot.scheduled_at,
                "token",
                "lease",
            ))
        } else {
            ExecutionGuardAcquire::Contended
        })
    }

    async fn renew(&self, _lease: &ExecutionLease) -> Result<ExecutionGuardRenewal, Self::Error> {
        Ok(ExecutionGuardRenewal::Renewed)
    }

    async fn release(&self, lease: &ExecutionLease) -> Result<(), Self::Error> {
        let mut state = self.state.lock().unwrap();
        match lease.scope {
            ExecutionGuardScope::Occurrence => {
                if let Some(entries) = state.occurrence_locks.get_mut(&lease.resource_id) {
                    if let Some(scheduled_at) = lease.scheduled_at {
                        entries.remove(&scheduled_at.to_rfc3339());
                    }
                }
            }
            ExecutionGuardScope::Resource => {
                state.resource_locks.remove(&lease.resource_id);
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct FakeCoordinatedStore {
    inner: Arc<Mutex<FakeCoordinatedStoreState>>,
}

#[derive(Clone)]
struct FakeCoordinatedStoreState {
    runtime: CoordinatedRuntimeState,
    inflight: Option<FakeInflight>,
}

#[derive(Clone)]
struct FakeInflight {
    trigger: CoordinatedPendingTrigger,
    resource_id: String,
    lease: ExecutionLease,
    expires_at: Instant,
}

impl FakeCoordinatedStore {
    fn new(state: JobState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeCoordinatedStoreState {
                runtime: CoordinatedRuntimeState { state, revision: 0 },
                inflight: None,
            })),
        }
    }
}

impl CoordinatedStateStore for FakeCoordinatedStore {
    type Error = Infallible;

    async fn load_or_initialize(
        &self,
        _job_id: &str,
        _initial_state: JobState,
    ) -> Result<CoordinatedRuntimeState, Self::Error> {
        Ok(self.inner.lock().unwrap().runtime.clone())
    }

    async fn save_state(
        &self,
        _job_id: &str,
        revision: u64,
        state: &JobState,
    ) -> Result<bool, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        if inner.runtime.revision != revision || inner.inflight.is_some() {
            return Ok(false);
        }
        inner.runtime.revision += 1;
        inner.runtime.state = state.clone();
        Ok(true)
    }

    async fn reclaim_inflight(
        &self,
        job_id: &str,
        resource_id: &str,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        let Some(inflight) = inner.inflight.clone() else {
            return Ok(None);
        };
        if inflight.expires_at > Instant::now() {
            return Ok(None);
        }

        inner.runtime.revision += 1;
        let lease = ExecutionLease::new(
            job_id.to_string(),
            resource_id.to_string(),
            ExecutionGuardScope::Occurrence,
            Some(inflight.trigger.scheduled_at),
            "reclaimed-token",
            "reclaimed-lease",
        );
        inner.inflight = Some(FakeInflight {
            trigger: inflight.trigger.clone(),
            resource_id: inflight.resource_id,
            expires_at: Instant::now() + lease_config.ttl,
            lease: lease.clone(),
        });

        Ok(Some(CoordinatedClaim {
            state: inner.runtime.clone(),
            trigger: inflight.trigger,
            lease,
            replayed: true,
        }))
    }

    async fn claim_trigger(
        &self,
        job_id: &str,
        resource_id: &str,
        revision: u64,
        trigger: CoordinatedPendingTrigger,
        next_state: &JobState,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        if inner.runtime.revision != revision || inner.inflight.is_some() {
            return Ok(None);
        }

        inner.runtime.revision += 1;
        inner.runtime.state = next_state.clone();
        let lease = ExecutionLease::new(
            job_id.to_string(),
            resource_id.to_string(),
            ExecutionGuardScope::Occurrence,
            Some(trigger.scheduled_at),
            "claim-token",
            "claim-lease",
        );
        inner.inflight = Some(FakeInflight {
            trigger: trigger.clone(),
            resource_id: resource_id.to_string(),
            expires_at: Instant::now() + lease_config.ttl,
            lease: lease.clone(),
        });

        Ok(Some(CoordinatedClaim {
            state: inner.runtime.clone(),
            trigger,
            lease,
            replayed: false,
        }))
    }

    async fn renew(
        &self,
        lease: &ExecutionLease,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<ExecutionGuardRenewal, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        let Some(inflight) = inner.inflight.as_mut() else {
            return Ok(ExecutionGuardRenewal::Lost);
        };
        if inflight.lease.token != lease.token {
            return Ok(ExecutionGuardRenewal::Lost);
        }
        inflight.expires_at = Instant::now() + lease_config.ttl;
        Ok(ExecutionGuardRenewal::Renewed)
    }

    async fn complete(
        &self,
        _job_id: &str,
        revision: u64,
        lease: &ExecutionLease,
        state: &JobState,
    ) -> Result<bool, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        if inner.runtime.revision != revision {
            return Ok(false);
        }
        if inner
            .inflight
            .as_ref()
            .map(|value| value.lease.token.as_str())
            != Some(lease.token.as_str())
        {
            return Ok(false);
        }
        inner.runtime.revision += 1;
        inner.runtime.state = state.clone();
        inner.inflight = None;
        Ok(true)
    }

    async fn delete(&self, _job_id: &str) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.runtime.state.next_run_at = None;
        inner.inflight = None;
        Ok(())
    }
}

use scheduler::JobState;

#[derive(Clone, Default)]
struct RecordingObserver {
    events: Arc<Mutex<Vec<SchedulerEvent>>>,
}

impl RecordingObserver {
    fn snapshot(&self) -> Vec<SchedulerEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl SchedulerObserver for RecordingObserver {
    fn on_event(&self, event: &SchedulerEvent) {
        self.events.lock().unwrap().push(event.clone());
    }
}

#[tokio::test]
async fn guarded_runner_resource_scope_blocks_occurrence_scope_for_same_resource() {
    let guard = InMemoryScopeGuard::default();
    let runner = GuardedRunner::new(guard);

    let session = runner
        .acquire(ExecutionSlot::for_resource("manual", "shared-resource"))
        .await
        .unwrap()
        .expect("expected resource lock to be acquired");

    let contended = runner
        .run(
            ExecutionSlot::for_occurrence("scheduled", "shared-resource", Utc::now()),
            || async { 7u32 },
        )
        .await
        .unwrap();

    assert_eq!(contended, GuardedRunResult::Contended);
    assert_eq!(session.run(|| async {}).await, ());
}

#[tokio::test]
async fn coordinated_store_reclaims_expired_inflight_occurrence() {
    let store = FakeCoordinatedStore::new(JobState::new("job", Some(Utc::now())));
    let trigger = CoordinatedPendingTrigger {
        scheduled_at: Utc::now(),
        catch_up: false,
        trigger_count: 1,
    };
    let lease_config = CoordinatedLeaseConfig {
        ttl: Duration::from_millis(20),
        renew_interval: Duration::from_millis(5),
    };
    let runtime = store
        .load_or_initialize("job", JobState::new("job", Some(Utc::now())))
        .await
        .unwrap();
    let claim = store
        .claim_trigger(
            "job",
            "resource",
            runtime.revision,
            trigger.clone(),
            &runtime.state,
            lease_config,
        )
        .await
        .unwrap()
        .expect("expected initial claim");

    assert!(!claim.replayed);
    tokio::time::sleep(Duration::from_millis(25)).await;

    let replay = store
        .reclaim_inflight("job", "resource", lease_config)
        .await
        .unwrap()
        .expect("expected replay claim");

    assert!(replay.replayed);
    assert_eq!(replay.trigger.scheduled_at, trigger.scheduled_at);
    assert_eq!(replay.trigger.trigger_count, trigger.trigger_count);
}

#[tokio::test]
async fn coordinated_scheduler_runs_basic_at_time_job() {
    let when = Utc::now() + chrono::TimeDelta::milliseconds(20);
    let state = JobState::new("coord-job", Some(when));
    let store = FakeCoordinatedStore::new(state);
    let scheduler = Scheduler::with_coordinated_state_store(
        SchedulerConfig::default(),
        store,
        CoordinatedLeaseConfig {
            ttl: Duration::from_secs(1),
            renew_interval: Duration::from_millis(50),
        },
    );

    let report = scheduler
        .run(
            Job::without_deps(
                "coord-job",
                Schedule::AtTimes(vec![when.with_timezone(&chrono_tz::Asia::Shanghai)]),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_overlap_policy(OverlapPolicy::Forbid)
            .with_max_runs(1),
        )
        .await
        .unwrap();

    assert_eq!(report.history.len(), 1);
    assert_eq!(report.state.trigger_count, 1);
}

#[tokio::test]
async fn coordinated_scheduler_skips_outside_time_window() {
    let when = Utc::now() + chrono::TimeDelta::milliseconds(20);
    let state = JobState::new("coord-window-job", Some(when));
    let store = FakeCoordinatedStore::new(state);
    let observer = RecordingObserver::default();
    let scheduler = Scheduler::with_observer_and_coordinated_state_store(
        SchedulerConfig::default(),
        store,
        observer.clone(),
        CoordinatedLeaseConfig {
            ttl: Duration::from_secs(1),
            renew_interval: Duration::from_millis(50),
        },
    );

    let report = scheduler
        .run(
            Job::without_deps(
                "coord-window-job",
                Schedule::AtTimes(vec![when.with_timezone(&chrono_tz::Asia::Shanghai)]),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_time_window(JobTimeWindow {
                timezone: None,
                weekdays: vec![match Utc::now()
                    .with_timezone(&chrono_tz::Asia::Shanghai)
                    .weekday()
                {
                    chrono::Weekday::Mon => chrono::Weekday::Tue,
                    chrono::Weekday::Tue => chrono::Weekday::Wed,
                    chrono::Weekday::Wed => chrono::Weekday::Thu,
                    chrono::Weekday::Thu => chrono::Weekday::Fri,
                    chrono::Weekday::Fri => chrono::Weekday::Sat,
                    chrono::Weekday::Sat => chrono::Weekday::Sun,
                    chrono::Weekday::Sun => chrono::Weekday::Mon,
                }],
                segments: vec![],
            }),
        )
        .await
        .unwrap();

    let events = observer.snapshot();

    assert!(report.history.is_empty());
    assert_eq!(
        report.last_skip_reason,
        Some(RunSkipReason::OutsideTimeWindow)
    );
    assert!(events.iter().any(|event| matches!(
        event,
        SchedulerEvent::RunSkipped {
            job_id,
            reason,
            ..
        } if job_id == "coord-window-job" && *reason == RunSkipReason::OutsideTimeWindow
    )));
}
