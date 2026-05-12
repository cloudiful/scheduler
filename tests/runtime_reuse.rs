#[allow(dead_code)]
#[path = "support/guarded_and_coordinated_fixtures.rs"]
mod fixtures;
#[path = "support/time.rs"]
mod time_support;

use chrono::Utc;
use fixtures::{FakeCoordinatedStore, RecordingObserver};
use scheduler::{
    CoordinatedLeaseConfig, InMemoryStateStore, Job, JobState, OverlapPolicy, Schedule, Scheduler,
    SchedulerConfig, SchedulerEvent, Task,
};
use std::time::Duration;
use time_support::shanghai_after;

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
