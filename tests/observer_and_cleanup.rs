mod support;

use chrono::{TimeDelta, Utc};
use scheduler::{
    InMemoryStateStore, Job, JobState, NoopObserver, Schedule, Scheduler, SchedulerConfig,
    SchedulerEvent, SchedulerObserver, SchedulerStopReason, StateLoadSource, StateStore, Task,
    TerminalStatePolicy,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use support::shanghai_after;
use tokio::sync::mpsc;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observer_receives_repair_trigger_and_completion_events() {
    let store = Arc::new(InMemoryStateStore::new());
    store
        .save(&JobState {
            job_id: "observer-repair".to_string(),
            trigger_count: 2,
            last_run_at: Some(Utc::now() - TimeDelta::seconds(10)),
            last_success_at: Some(Utc::now() - TimeDelta::seconds(9)),
            next_run_at: None,
            last_error: Some("stale".to_string()),
        })
        .await
        .unwrap();

    let observer = RecordingObserver::default();
    let scheduler = Arc::new(Scheduler::with_observer(
        SchedulerConfig::default(),
        store.clone(),
        observer.clone(),
    ));
    let handle = scheduler.handle();
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let (tx, mut rx) = mpsc::channel::<()>(1);

    let task = {
        let scheduler = scheduler.clone();
        tokio::spawn(async move {
            scheduler
                .run(Job::without_deps(
                    "observer-repair",
                    Schedule::Interval(Duration::from_millis(80)),
                    Task::from_async(move |_| {
                        let tx = tx.clone();
                        let seen = seen.clone();
                        async move {
                            seen.fetch_add(1, Ordering::SeqCst);
                            let _ = tx.send(()).await;
                            Ok(())
                        }
                    }),
                ))
                .await
                .unwrap()
        })
    };

    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = rx.recv().await;
        shutdown_handle.shutdown();
    });

    let _report = task.await.unwrap();
    let events = observer.snapshot();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::StateRepaired { job_id, trigger_count, .. }
                if job_id == "observer-repair" && *trigger_count == 2
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::StateLoaded { job_id, source, .. }
                if job_id == "observer-repair" && *source == StateLoadSource::Repaired
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::TriggerEmitted { job_id, catch_up, .. }
                if job_id == "observer-repair" && !catch_up
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::RunCompleted { job_id, trigger_count, .. }
                if job_id == "observer-repair" && *trigger_count == 3
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::SchedulerStopped { job_id, reason, .. }
                if job_id == "observer-repair" && *reason == SchedulerStopReason::Shutdown
        )
    }));
}

#[tokio::test]
async fn terminal_state_policy_delete_removes_state_and_emits_event() {
    let store = Arc::new(InMemoryStateStore::new());
    let observer = RecordingObserver::default();
    let scheduler = Scheduler::with_observer(
        SchedulerConfig {
            terminal_state_policy: TerminalStatePolicy::Delete,
            ..SchedulerConfig::default()
        },
        store.clone(),
        observer.clone(),
    );

    let report = scheduler
        .run(
            Job::without_deps(
                "cleanup-terminal",
                Schedule::AtTimes(vec![shanghai_after(20)]),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_max_runs(1),
        )
        .await
        .unwrap();

    let persisted = store.load("cleanup-terminal").await.unwrap();
    let events = observer.snapshot();

    assert_eq!(report.state.trigger_count, 1);
    assert!(persisted.is_none());
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::TerminalStateDeleted { job_id, trigger_count }
                if job_id == "cleanup-terminal" && *trigger_count == 1
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::SchedulerStopped { job_id, reason, .. }
                if job_id == "cleanup-terminal" && *reason == SchedulerStopReason::Terminal
        )
    }));
}

#[test]
fn noop_observer_accepts_events_without_side_effects() {
    let observer = NoopObserver;
    observer.on_event(&SchedulerEvent::SchedulerStopped {
        job_id: "noop".to_string(),
        trigger_count: 0,
        reason: SchedulerStopReason::Terminal,
    });
}
