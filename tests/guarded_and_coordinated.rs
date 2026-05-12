#[path = "support/guarded_and_coordinated_fixtures.rs"]
mod fixtures;

use chrono::{Datelike, Utc};
use fixtures::{FakeCoordinatedStore, InMemoryScopeGuard, RecordingObserver};
use scheduler::{
    CoordinatedLeaseConfig, CoordinatedPendingTrigger, CoordinatedStateStore, ExecutionGuardScope,
    ExecutionSlot, GuardedRunResult, GuardedRunner, Job, JobState, JobTimeWindow, OverlapPolicy,
    PauseScope, RunSkipReason, Schedule, Scheduler, SchedulerConfig, SchedulerEvent, Task,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
            ExecutionGuardScope::Occurrence,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinated_scheduler_allows_parallel_occurrences() {
    let first = Utc::now() + chrono::TimeDelta::milliseconds(20);
    let second = first + chrono::TimeDelta::milliseconds(10);
    let state = JobState::new("coord-parallel", Some(first));
    let store = FakeCoordinatedStore::new(state);
    let scheduler = Scheduler::with_coordinated_state_store(
        SchedulerConfig::default(),
        store,
        CoordinatedLeaseConfig {
            ttl: Duration::from_secs(1),
            renew_interval: Duration::from_millis(50),
        },
    );
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));

    let report = scheduler
        .run(
            Job::without_deps(
                "coord-parallel",
                Schedule::AtTimes(vec![
                    first.with_timezone(&chrono_tz::Asia::Shanghai),
                    second.with_timezone(&chrono_tz::Asia::Shanghai),
                ]),
                Task::from_async({
                    let active = active.clone();
                    let max_active = max_active.clone();
                    move |_| {
                        let active = active.clone();
                        let max_active = max_active.clone();
                        async move {
                            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                            max_active.fetch_max(current, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(80)).await;
                            active.fetch_sub(1, Ordering::SeqCst);
                            Ok(())
                        }
                    }
                }),
            )
            .with_overlap_policy(OverlapPolicy::AllowParallel)
            .with_max_runs(2),
        )
        .await
        .unwrap();

    assert_eq!(report.history.len(), 2);
    assert!(max_active.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn coordinated_store_resource_scope_blocks_parallel_claims() {
    let first = Utc::now();
    let second = first + chrono::TimeDelta::milliseconds(10);
    let mut state = JobState::new("coord-resource", Some(first));
    let store = FakeCoordinatedStore::new(state.clone());
    let config = CoordinatedLeaseConfig {
        ttl: Duration::from_secs(1),
        renew_interval: Duration::from_millis(50),
    };
    let runtime = store
        .load_or_initialize("coord-resource", state.clone())
        .await
        .unwrap();
    let first_claim = store
        .claim_trigger(
            "coord-resource",
            "resource",
            runtime.revision,
            CoordinatedPendingTrigger {
                scheduled_at: first,
                catch_up: false,
                trigger_count: 1,
            },
            &state,
            config,
            ExecutionGuardScope::Resource,
        )
        .await
        .unwrap();
    assert!(first_claim.is_some());

    state.trigger_count = 2;
    let blocked = store
        .claim_trigger(
            "coord-resource",
            "resource",
            runtime.revision + 1,
            CoordinatedPendingTrigger {
                scheduled_at: second,
                catch_up: false,
                trigger_count: 2,
            },
            &state,
            config,
            ExecutionGuardScope::Resource,
        )
        .await
        .unwrap();
    assert!(blocked.is_none());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinated_pause_is_shared_across_instances_and_emits_shared_events() {
    let when = Utc::now() + chrono::TimeDelta::milliseconds(20);
    let state = JobState::new("coord-shared-pause", Some(when));
    let store = FakeCoordinatedStore::new(state);
    let observer = RecordingObserver::default();
    let lease_config = CoordinatedLeaseConfig {
        ttl: Duration::from_secs(1),
        renew_interval: Duration::from_millis(50),
    };
    let scheduler_one = Arc::new(Scheduler::with_observer_and_coordinated_state_store(
        SchedulerConfig::default(),
        store.clone(),
        observer.clone(),
        lease_config,
    ));
    let scheduler_two = Arc::new(Scheduler::with_coordinated_state_store(
        SchedulerConfig::default(),
        store.clone(),
        lease_config,
    ));
    let handle = scheduler_one.handle();
    let invocations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = invocations.clone();

    let paused_run = {
        let scheduler = scheduler_one.clone();
        tokio::spawn(async move {
            scheduler
                .run(
                    Job::without_deps(
                        "coord-shared-pause",
                        Schedule::AtTimes(vec![when.with_timezone(&chrono_tz::Asia::Shanghai)]),
                        Task::from_async(move |_| {
                            let seen = seen.clone();
                            async move {
                                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                Ok(())
                            }
                        }),
                    )
                    .with_overlap_policy(OverlapPolicy::Forbid)
                    .with_max_runs(1),
                )
                .await
                .unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(5)).await;
    handle.pause().await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(invocations.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(store.is_paused());

    handle.resume().await.unwrap();
    let report = paused_run.await.unwrap();
    assert_eq!(report.history.len(), 1);
    assert_eq!(invocations.load(std::sync::atomic::Ordering::SeqCst), 1);

    let events = observer.snapshot();
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::SchedulerPaused { job_id, scope, .. }
                if job_id == "coord-shared-pause" && *scope == PauseScope::Shared
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::SchedulerResumed { job_id, scope, .. }
                if job_id == "coord-shared-pause" && *scope == PauseScope::Shared
        )
    }));

    let second_when = Utc::now() + chrono::TimeDelta::milliseconds(20);
    store.reset_runtime_for_test(Some(second_when), 0, true);
    invocations.store(0, std::sync::atomic::Ordering::SeqCst);

    let blocked_run = {
        let scheduler = scheduler_two.clone();
        tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                scheduler.run(
                    Job::without_deps(
                        "coord-shared-pause",
                        Schedule::AtTimes(vec![
                            second_when.with_timezone(&chrono_tz::Asia::Shanghai),
                        ]),
                        Task::from_async(|_| async { Ok(()) }),
                    )
                    .with_overlap_policy(OverlapPolicy::Forbid)
                    .with_max_runs(1),
                ),
            )
            .await
        })
    };

    let blocked = blocked_run.await.unwrap();
    assert!(blocked.is_err());
    assert_eq!(invocations.load(std::sync::atomic::Ordering::SeqCst), 0);
}
