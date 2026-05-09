#[path = "support/execution_guard_fixtures.rs"]
mod fixtures;
#[path = "support/time.rs"]
mod time_support;

use fixtures::{AcquirePlan, FakeExecutionGuard, RecordingObserver, ReleasePlan, RenewPlan};
use scheduler::{
    ExecutionGuardErrorKind, InMemoryStateStore, Job, NoopExecutionGuard, OverlapPolicy, Schedule,
    Scheduler, SchedulerConfig, SchedulerError, SchedulerEvent, SchedulerStopReason, Task,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use time_support::shanghai_after;

#[cfg(feature = "valkey-guard")]
use scheduler::{ValkeyExecutionGuard, ValkeyLeaseConfig};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_error_stops_future_triggers_and_shuts_down() {
    let observer = RecordingObserver::default();
    let scheduler = Scheduler::with_observer_and_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        observer.clone(),
        FakeExecutionGuard::new(
            [],
            [RenewPlan::Error(
                ExecutionGuardErrorKind::Connection,
                "renew connection failed",
            )],
            [],
            Some(Duration::from_millis(20)),
        ),
    );

    let report = scheduler
        .run(
            Job::without_deps(
                "guard-renew-error",
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
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::ExecutionGuardRenewFailed { job_id, error, .. }
                if job_id == "guard-renew-error" && error == "renew connection failed"
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::ExecutionGuardLost { job_id, .. } if job_id == "guard-renew-error"
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::SchedulerStopped { job_id, reason, .. }
                if job_id == "guard-renew-error" && *reason == SchedulerStopReason::Shutdown
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
