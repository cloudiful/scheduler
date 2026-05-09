#[path = "support/time.rs"]
mod time_support;

use chrono::Utc;
use scheduler::{
    CoordinatedClaim, CoordinatedLeaseConfig, CoordinatedPendingTrigger, CoordinatedRuntimeState,
    CoordinatedStateStore, ExecutionGuardRenewal, ExecutionGuardScope, ExecutionLease,
    InMemoryStateStore, Job, JobState, OverlapPolicy, Schedule, Scheduler, SchedulerConfig,
    SchedulerEvent, SchedulerObserver, Task,
};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use time_support::shanghai_after;

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
    _trigger: CoordinatedPendingTrigger,
    _resource_id: String,
    lease: ExecutionLease,
    expires_at: Instant,
}

impl FakeCoordinatedStore {
    fn new(state: JobState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeCoordinatedStoreState {
                runtime: CoordinatedRuntimeState {
                    state,
                    revision: 0,
                    paused: false,
                },
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
        _job_id: &str,
        _resource_id: &str,
        _lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        Ok(None)
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
        if inner.runtime.paused || inner.runtime.revision != revision || inner.inflight.is_some() {
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
            _trigger: trigger.clone(),
            _resource_id: resource_id.to_string(),
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
        Ok(())
    }

    async fn pause(&self, _job_id: &str) -> Result<bool, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        let changed = !inner.runtime.paused;
        inner.runtime.paused = true;
        Ok(changed)
    }

    async fn resume(&self, _job_id: &str) -> Result<bool, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        let changed = inner.runtime.paused;
        inner.runtime.paused = false;
        Ok(changed)
    }
}

fn core_event_kinds(events: &[SchedulerEvent]) -> Vec<&'static str> {
    events
        .iter()
        .filter_map(|event| match event {
            SchedulerEvent::StateLoaded { .. } => Some("state_loaded"),
            SchedulerEvent::TriggerEmitted { .. } => Some("trigger_emitted"),
            SchedulerEvent::RunCompleted { .. } => Some("run_completed"),
            SchedulerEvent::SchedulerStopped { .. } => Some("scheduler_stopped"),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn legacy_and_coordinated_share_core_event_sequence() {
    let when = shanghai_after(20);

    let legacy_observer = RecordingObserver::default();
    let legacy_scheduler = Scheduler::with_observer(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        legacy_observer.clone(),
    );
    let legacy_report = legacy_scheduler
        .run(
            Job::without_deps(
                "runtime-reuse-legacy",
                Schedule::AtTimes(vec![when]),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_overlap_policy(OverlapPolicy::Forbid)
            .with_max_runs(1),
        )
        .await
        .unwrap();

    let coordinated_observer = RecordingObserver::default();
    let coordinated_scheduler = Scheduler::with_observer_and_coordinated_state_store(
        SchedulerConfig::default(),
        FakeCoordinatedStore::new(JobState::new(
            "runtime-reuse-coordinated",
            Some(when.with_timezone(&Utc)),
        )),
        coordinated_observer.clone(),
        CoordinatedLeaseConfig {
            ttl: Duration::from_secs(1),
            renew_interval: Duration::from_millis(50),
        },
    );
    let coordinated_report = coordinated_scheduler
        .run(
            Job::without_deps(
                "runtime-reuse-coordinated",
                Schedule::AtTimes(vec![when]),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_overlap_policy(OverlapPolicy::Forbid)
            .with_max_runs(1),
        )
        .await
        .unwrap();

    assert_eq!(legacy_report.history.len(), 1);
    assert_eq!(coordinated_report.history.len(), 1);
    assert_eq!(
        core_event_kinds(&legacy_observer.snapshot()),
        vec![
            "state_loaded",
            "trigger_emitted",
            "run_completed",
            "scheduler_stopped"
        ]
    );
    assert_eq!(
        core_event_kinds(&coordinated_observer.snapshot()),
        vec![
            "state_loaded",
            "trigger_emitted",
            "run_completed",
            "scheduler_stopped"
        ]
    );
    let coordinated_events = coordinated_observer.snapshot();
    assert!(
        coordinated_events
            .iter()
            .any(|event| matches!(event, SchedulerEvent::ExecutionGuardAcquired { .. }))
    );
    assert!(
        coordinated_events
            .iter()
            .any(|event| matches!(event, SchedulerEvent::ExecutionGuardReleased { .. }))
    );
}
