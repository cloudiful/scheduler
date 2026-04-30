mod support;

#[cfg(feature = "valkey-guard")]
use chrono::Utc;
use scheduler::{
    ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardErrorKind, ExecutionGuardRenewal,
    ExecutionLease, ExecutionSlot, InMemoryStateStore, Job, NoopExecutionGuard, OverlapPolicy,
    Schedule, Scheduler, SchedulerConfig, SchedulerError, SchedulerEvent, SchedulerObserver,
    SchedulerStopReason, Task,
};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use support::shanghai_after;

#[cfg(feature = "valkey-guard")]
use redis::{AsyncCommands, Client};
#[cfg(feature = "valkey-guard")]
use scheduler::{ValkeyExecutionGuard, ValkeyLeaseConfig};

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

#[derive(Debug, Clone)]
struct FakeGuardError {
    kind: ExecutionGuardErrorKind,
    message: &'static str,
}

impl Display for FakeGuardError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}

impl Error for FakeGuardError {}

#[derive(Debug, Clone, Copy)]
enum AcquirePlan {
    Acquired,
    Contended,
    Error(ExecutionGuardErrorKind, &'static str),
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
enum RenewPlan {
    Renewed,
    Lost,
    Error(ExecutionGuardErrorKind, &'static str),
}

#[derive(Debug, Clone, Copy)]
enum ReleasePlan {
    Ok,
    Error(ExecutionGuardErrorKind, &'static str),
}

#[derive(Clone, Default)]
struct FakeExecutionGuard {
    state: Arc<Mutex<FakeExecutionGuardState>>,
    renew_every: Option<Duration>,
}

#[derive(Default)]
struct FakeExecutionGuardState {
    acquire_plan: VecDeque<AcquirePlan>,
    renew_plan: VecDeque<RenewPlan>,
    release_plan: VecDeque<ReleasePlan>,
    slots: Vec<ExecutionSlot>,
    acquire_count: usize,
}

impl FakeExecutionGuard {
    fn new(
        acquire_plan: impl IntoIterator<Item = AcquirePlan>,
        renew_plan: impl IntoIterator<Item = RenewPlan>,
        release_plan: impl IntoIterator<Item = ReleasePlan>,
        renew_every: Option<Duration>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeExecutionGuardState {
                acquire_plan: acquire_plan.into_iter().collect(),
                renew_plan: renew_plan.into_iter().collect(),
                release_plan: release_plan.into_iter().collect(),
                slots: Vec::new(),
                acquire_count: 0,
            })),
            renew_every,
        }
    }

    fn slots(&self) -> Vec<ExecutionSlot> {
        self.state.lock().unwrap().slots.clone()
    }

    fn acquire_count(&self) -> usize {
        self.state.lock().unwrap().acquire_count
    }
}

impl ExecutionGuard for FakeExecutionGuard {
    type Error = FakeGuardError;

    async fn acquire(&self, slot: ExecutionSlot) -> Result<ExecutionGuardAcquire, Self::Error> {
        let mut state = self.state.lock().unwrap();
        state.acquire_count += 1;
        state.slots.push(slot.clone());
        let plan = state
            .acquire_plan
            .pop_front()
            .unwrap_or(AcquirePlan::Acquired);

        match plan {
            AcquirePlan::Acquired => Ok(ExecutionGuardAcquire::Acquired(ExecutionLease::new(
                slot.job_id.clone(),
                slot.resource_id.clone(),
                slot.scope,
                slot.scheduled_at,
                format!("token-{}", state.acquire_count),
                match slot.scheduled_at {
                    Some(scheduled_at) => {
                        format!("lease:{}:{}", slot.resource_id, scheduled_at.to_rfc3339())
                    }
                    None => format!("lease:{}:resource", slot.resource_id),
                },
            ))),
            AcquirePlan::Contended => Ok(ExecutionGuardAcquire::Contended),
            AcquirePlan::Error(kind, message) => Err(FakeGuardError { kind, message }),
        }
    }

    async fn renew(&self, _lease: &ExecutionLease) -> Result<ExecutionGuardRenewal, Self::Error> {
        let plan = self
            .state
            .lock()
            .unwrap()
            .renew_plan
            .pop_front()
            .unwrap_or(RenewPlan::Renewed);

        match plan {
            RenewPlan::Renewed => Ok(ExecutionGuardRenewal::Renewed),
            RenewPlan::Lost => Ok(ExecutionGuardRenewal::Lost),
            RenewPlan::Error(kind, message) => Err(FakeGuardError { kind, message }),
        }
    }

    async fn release(&self, _lease: &ExecutionLease) -> Result<(), Self::Error> {
        let plan = self
            .state
            .lock()
            .unwrap()
            .release_plan
            .pop_front()
            .unwrap_or(ReleasePlan::Ok);

        match plan {
            ReleasePlan::Ok => Ok(()),
            ReleasePlan::Error(kind, message) => Err(FakeGuardError { kind, message }),
        }
    }

    fn classify_error(error: &Self::Error) -> ExecutionGuardErrorKind
    where
        Self: Sized,
    {
        error.kind
    }

    fn renew_interval(&self, _lease: &ExecutionLease) -> Option<Duration> {
        self.renew_every
    }
}

#[tokio::test]
async fn noop_execution_guard_keeps_existing_behavior() {
    let scheduler = Scheduler::with_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        NoopExecutionGuard,
    );
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let report = scheduler
        .run(
            Job::without_deps(
                "noop-guard",
                Schedule::AtTimes(vec![shanghai_after(20)]),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }),
            )
            .with_max_runs(1),
        )
        .await
        .unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
}

#[tokio::test]
async fn contended_guard_skips_run_and_emits_event() {
    let observer = RecordingObserver::default();
    let scheduler = Scheduler::with_observer_and_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        observer.clone(),
        FakeExecutionGuard::new([AcquirePlan::Contended], [], [], None),
    );
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let report = scheduler
        .run(
            Job::without_deps(
                "guard-contended",
                Schedule::AtTimes(vec![shanghai_after(20)]),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }),
            )
            .with_max_runs(1),
        )
        .await
        .unwrap();

    let events = observer.snapshot();
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    assert!(report.history.is_empty());
    assert!(report.state.trigger_count >= 1);
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::ExecutionGuardContended { job_id, .. } if job_id == "guard-contended"
        )
    }));
}

#[tokio::test]
async fn acquire_error_returns_execution_guard_scheduler_error() {
    let scheduler = Scheduler::with_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        FakeExecutionGuard::new(
            [AcquirePlan::Error(
                ExecutionGuardErrorKind::Connection,
                "guard connection failed",
            )],
            [],
            [],
            None,
        ),
    );

    let error = scheduler
        .run(
            Job::without_deps(
                "guard-error",
                Schedule::AtTimes(vec![shanghai_after(20)]),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_max_runs(1),
        )
        .await
        .unwrap_err();

    match error {
        SchedulerError::ExecutionGuard(error) => {
            assert_eq!(error.kind(), ExecutionGuardErrorKind::Connection);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forbid_drops_before_guard_acquire() {
    let guard = FakeExecutionGuard::new([], [], [], None);
    let scheduler = Scheduler::with_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        guard.clone(),
    );

    let report = scheduler
        .run(
            Job::without_deps(
                "guard-forbid",
                Schedule::AtTimes(vec![shanghai_after(20), shanghai_after(40)]),
                Task::from_async(|_| async {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Ok(())
                }),
            )
            .with_overlap_policy(OverlapPolicy::Forbid),
        )
        .await
        .unwrap();

    assert_eq!(guard.acquire_count(), 1);
    assert_eq!(report.state.trigger_count, 2);
    assert_eq!(report.history.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_trigger_acquires_only_when_dequeued() {
    let guard = FakeExecutionGuard::new([], [], [], None);
    let scheduler = Scheduler::with_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        guard.clone(),
    );
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let acquire_count = guard.clone();

    let report = scheduler
        .run(
            Job::without_deps(
                "guard-queue-one",
                Schedule::AtTimes(vec![shanghai_after(20), shanghai_after(40)]),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    let acquire_count = acquire_count.clone();
                    async move {
                        let invocation = seen.fetch_add(1, Ordering::SeqCst);
                        if invocation == 0 {
                            tokio::time::sleep(Duration::from_millis(80)).await;
                            assert_eq!(acquire_count.acquire_count(), 1);
                        }
                        Ok(())
                    }
                }),
            )
            .with_overlap_policy(OverlapPolicy::QueueOne),
        )
        .await
        .unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(guard.acquire_count(), 2);
    assert_eq!(report.history.len(), 2);
}

#[tokio::test]
async fn release_error_only_emits_event() {
    let observer = RecordingObserver::default();
    let scheduler = Scheduler::with_observer_and_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        observer.clone(),
        FakeExecutionGuard::new(
            [],
            [],
            [ReleasePlan::Error(
                ExecutionGuardErrorKind::Connection,
                "release failed",
            )],
            None,
        ),
    );

    let report = scheduler
        .run(
            Job::without_deps(
                "guard-release-error",
                Schedule::AtTimes(vec![shanghai_after(20)]),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_max_runs(1),
        )
        .await
        .unwrap();

    let events = observer.snapshot();
    assert_eq!(report.history.len(), 1);
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::ExecutionGuardReleaseFailed { job_id, error, .. }
                if job_id == "guard-release-error" && error == "release failed"
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::RunCompleted { job_id, .. } if job_id == "guard-release-error"
        )
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_renewal_stops_future_triggers_and_shuts_down() {
    let observer = RecordingObserver::default();
    let scheduler = Scheduler::with_observer_and_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        observer.clone(),
        FakeExecutionGuard::new([], [RenewPlan::Lost], [], Some(Duration::from_millis(20))),
    );

    let report = scheduler
        .run(
            Job::without_deps(
                "guard-lost",
                Schedule::Interval(Duration::from_millis(10)),
                Task::from_async(|_| async {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Ok(())
                }),
            )
            .with_max_runs(10),
        )
        .await
        .unwrap();

    let events = observer.snapshot();
    assert_eq!(report.history.len(), 1);
    assert!(report.state.trigger_count >= 1);
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::ExecutionGuardLost { job_id, .. } if job_id == "guard-lost"
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::SchedulerStopped { job_id, reason, .. }
                if job_id == "guard-lost" && *reason == SchedulerStopReason::Shutdown
        )
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allow_parallel_uses_distinct_slots_per_occurrence() {
    let guard = FakeExecutionGuard::new([], [], [], None);
    let scheduler = Scheduler::with_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        guard.clone(),
    );

    let report = scheduler
        .run(
            Job::without_deps(
                "guard-parallel",
                Schedule::AtTimes(vec![shanghai_after(20), shanghai_after(40)]),
                Task::from_async(|_| async {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Ok(())
                }),
            )
            .with_overlap_policy(OverlapPolicy::AllowParallel),
        )
        .await
        .unwrap();

    let slots = guard.slots();
    assert_eq!(report.history.len(), 2);
    assert_eq!(slots.len(), 2);
    assert_eq!(slots[0].job_id, "guard-parallel");
    assert_eq!(slots[1].job_id, "guard-parallel");
    assert_ne!(slots[0].scheduled_at, slots[1].scheduled_at);
}

#[cfg(feature = "valkey-guard")]
fn valkey_url() -> Option<String> {
    std::env::var("SCHEDULER_VALKEY_URL").ok()
}

#[cfg(feature = "valkey-guard")]
fn unique_id() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    format!("scheduler-execution-guard-{}-{now}", std::process::id())
}

#[cfg(feature = "valkey-guard")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn same_occurrence_runs_on_only_one_scheduler_instance() {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let prefix = format!("scheduler:test:execution-guard:{}:", unique_id());
    let planned = shanghai_after(120);
    let invocations = Arc::new(AtomicUsize::new(0));

    let scheduler_one = Scheduler::with_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        ValkeyExecutionGuard::with_prefix(
            &url,
            prefix.clone(),
            ValkeyLeaseConfig {
                ttl: Duration::from_secs(5),
                renew_interval: Duration::from_secs(1),
            },
        )
        .await
        .expect("failed to create first guard"),
    );
    let scheduler_two = Scheduler::with_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        ValkeyExecutionGuard::with_prefix(
            &url,
            prefix.clone(),
            ValkeyLeaseConfig {
                ttl: Duration::from_secs(5),
                renew_interval: Duration::from_secs(1),
            },
        )
        .await
        .expect("failed to create second guard"),
    );

    let job_one = {
        let seen = invocations.clone();
        Job::without_deps(
            "shared-job",
            Schedule::AtTimes(vec![planned]),
            Task::from_async(move |_| {
                let seen = seen.clone();
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(())
                }
            }),
        )
        .with_max_runs(1)
    };

    let job_two = {
        let seen = invocations.clone();
        Job::without_deps(
            "shared-job",
            Schedule::AtTimes(vec![planned]),
            Task::from_async(move |_| {
                let seen = seen.clone();
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(())
                }
            }),
        )
        .with_max_runs(1)
    };

    let (first, second) = tokio::join!(scheduler_one.run(job_one), scheduler_two.run(job_two));
    let first = first.expect("first scheduler run failed");
    let second = second.expect("second scheduler run failed");

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(first.history.len() + second.history.len(), 1);
}

#[cfg(feature = "valkey-guard")]
#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn different_occurrences_for_same_job_can_both_acquire() {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let prefix = format!("scheduler:test:execution-guard:{}:", unique_id());
    let guard = ValkeyExecutionGuard::with_prefix(
        &url,
        prefix.clone(),
        ValkeyLeaseConfig {
            ttl: Duration::from_secs(5),
            renew_interval: Duration::from_secs(1),
        },
    )
    .await
    .expect("failed to create guard");
    let first_slot = ExecutionSlot::new("shared-job", Utc::now());
    let second_slot = ExecutionSlot::new("shared-job", Utc::now() + chrono::TimeDelta::seconds(1));

    let first = guard
        .acquire(first_slot.clone())
        .await
        .expect("first acquire failed");
    let second = guard
        .acquire(second_slot.clone())
        .await
        .expect("second acquire failed");

    let ExecutionGuardAcquire::Acquired(first_lease) = first else {
        panic!("first occurrence should acquire");
    };
    let ExecutionGuardAcquire::Acquired(second_lease) = second else {
        panic!("second occurrence should acquire");
    };

    assert_ne!(first_lease.lease_key, second_lease.lease_key);
    guard
        .release(&first_lease)
        .await
        .expect("first release failed");
    guard
        .release(&second_lease)
        .await
        .expect("second release failed");
}

#[cfg(feature = "valkey-guard")]
#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn renew_returns_lost_when_token_no_longer_matches() {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let prefix = format!("scheduler:test:execution-guard:{}:", unique_id());
    let guard = ValkeyExecutionGuard::with_prefix(
        &url,
        prefix.clone(),
        ValkeyLeaseConfig {
            ttl: Duration::from_secs(5),
            renew_interval: Duration::from_secs(1),
        },
    )
    .await
    .expect("failed to create guard");
    let slot = ExecutionSlot::new("shared-job", Utc::now());
    let lease = match guard.acquire(slot).await.expect("acquire failed") {
        ExecutionGuardAcquire::Acquired(lease) => lease,
        ExecutionGuardAcquire::Contended => panic!("expected acquired lease"),
    };
    let client = Client::open(url).expect("invalid valkey url");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to get valkey connection");

    let _: () = connection
        .set(&lease.lease_key, "other-token")
        .await
        .expect("failed to replace token");

    let renewal = guard.renew(&lease).await.expect("renew should not error");
    assert_eq!(renewal, ExecutionGuardRenewal::Lost);

    let _: usize = connection
        .del(&lease.lease_key)
        .await
        .expect("cleanup failed");
}

#[cfg(feature = "valkey-guard")]
#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn release_only_deletes_the_owner_token() {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let prefix = format!("scheduler:test:execution-guard:{}:", unique_id());
    let guard = ValkeyExecutionGuard::with_prefix(
        &url,
        prefix.clone(),
        ValkeyLeaseConfig {
            ttl: Duration::from_secs(5),
            renew_interval: Duration::from_secs(1),
        },
    )
    .await
    .expect("failed to create guard");
    let slot = ExecutionSlot::new("shared-job", Utc::now());
    let lease = match guard.acquire(slot).await.expect("acquire failed") {
        ExecutionGuardAcquire::Acquired(lease) => lease,
        ExecutionGuardAcquire::Contended => panic!("expected acquired lease"),
    };
    let client = Client::open(url).expect("invalid valkey url");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to get valkey connection");

    let _: () = connection
        .set(&lease.lease_key, "foreign-token")
        .await
        .expect("failed to replace token");
    guard
        .release(&lease)
        .await
        .expect("release should not error");

    let remaining: Option<String> = connection
        .get(&lease.lease_key)
        .await
        .expect("failed to inspect lease key");
    assert_eq!(remaining.as_deref(), Some("foreign-token"));

    let _: usize = connection
        .del(&lease.lease_key)
        .await
        .expect("cleanup failed");
}
