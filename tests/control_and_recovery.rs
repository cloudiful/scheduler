#[path = "support/time.rs"]
mod time_support;

use chrono::{TimeDelta, Utc};
use chrono_tz::UTC;
use scheduler::{
    InMemoryStateStore, Job, JobState, PauseScope, Schedule, Scheduler, SchedulerConfig,
    SchedulerEvent, SchedulerObserver, StateStore, Task, TriggerSource, TriggeredTaskContext,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use time_support::shanghai_after;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

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
async fn state_is_restored_after_graceful_shutdown() {
    let store = Arc::new(InMemoryStateStore::new());
    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let handle = scheduler.handle();
    let (tx, mut rx) = mpsc::channel::<()>(1);
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let times = vec![shanghai_after(30), shanghai_after(120)];

    let job = Job::without_deps(
        "restore-state",
        Schedule::AtTimes(times.clone()),
        Task::from_async(move |_| {
            let tx = tx.clone();
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                let _ = tx.send(()).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }
        }),
    );

    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = rx.recv().await;
        shutdown_handle.shutdown();
    });

    let first_report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(first_report.history.len(), 1);

    let saved_state = store.load("restore-state").await.unwrap().unwrap();
    assert!(saved_state.next_run_at.is_some());

    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let seen = invocations.clone();
    let job = Job::without_deps(
        "restore-state",
        Schedule::AtTimes(times),
        Task::from_async(move |_| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    );

    let second_report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(second_report.history.len(), 1);
    assert_eq!(second_report.state.trigger_count, 2);
}

#[tokio::test]
async fn shanghai_schedule_is_respected_even_with_non_shanghai_config() {
    let scheduler = Scheduler::new(
        SchedulerConfig {
            timezone: UTC,
            history_limit: 8,
            ..SchedulerConfig::default()
        },
        InMemoryStateStore::new(),
    );
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let planned = shanghai_after(70);
    let planned_utc = planned.with_timezone(&chrono::Utc);

    let job = Job::without_deps(
        "timezone-explicit",
        Schedule::AtTimes(vec![planned]),
        Task::from_async(move |context| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                assert_eq!(context.run.scheduled_at, planned_utc);
                Ok(())
            }
        }),
    );

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history[0].scheduled_at, planned_utc);
}

#[tokio::test]
async fn cancel_stops_while_waiting() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let handle = scheduler.handle();

    let task = tokio::spawn(async move {
        scheduler
            .run(
                Job::without_deps(
                    "cancel-waiting",
                    Schedule::Interval(Duration::from_millis(200)),
                    Task::from_async(|_| async { Ok(()) }),
                )
                .with_max_runs(1),
            )
            .await
            .unwrap()
    });

    tokio::time::sleep(Duration::from_millis(40)).await;
    handle.cancel();

    let report = task.await.unwrap();

    assert!(report.history.is_empty());
    assert_eq!(report.state.trigger_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_the_running_task_and_persists_state() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let handle = scheduler.handle();
    let (tx, mut rx) = mpsc::channel::<()>(1);

    let job = Job::without_deps(
        "shutdown-running",
        Schedule::Interval(Duration::from_millis(10)),
        Task::from_async(move |_| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(()).await;
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok(())
            }
        }),
    )
    .with_max_runs(10);

    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = rx.recv().await;
        shutdown_handle.shutdown();
    });

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(report.history.len(), 1);
    assert_eq!(report.state.trigger_count, 1);
    assert!(report.state.last_success_at.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interval_state_with_missing_next_run_at_is_repaired_and_runs_again() {
    let store = Arc::new(InMemoryStateStore::new());
    let original_last_run_at = Utc::now() - TimeDelta::seconds(10);
    let original_last_success_at = Utc::now() - TimeDelta::seconds(9);
    let original_last_error = Some("previous failure".to_string());
    store
        .save(&JobState {
            job_id: "repair-interval-state".to_string(),
            trigger_count: 3,
            last_run_at: Some(original_last_run_at),
            last_success_at: Some(original_last_success_at),
            next_run_at: None,
            last_error: original_last_error.clone(),
        })
        .await
        .unwrap();

    let scheduler = Arc::new(Scheduler::new(SchedulerConfig::default(), store.clone()));
    let handle = scheduler.handle();
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let (tx, mut rx) = mpsc::channel::<()>(1);

    let task = {
        let scheduler = scheduler.clone();
        tokio::spawn(async move {
            scheduler
                .run(Job::without_deps(
                    "repair-interval-state",
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

    tokio::time::sleep(Duration::from_millis(20)).await;

    let repaired_state = store.load("repair-interval-state").await.unwrap().unwrap();
    assert_eq!(repaired_state.trigger_count, 3);
    assert_eq!(repaired_state.last_run_at, Some(original_last_run_at));
    assert_eq!(
        repaired_state.last_success_at,
        Some(original_last_success_at)
    );
    assert_eq!(repaired_state.last_error, original_last_error);
    assert!(repaired_state.next_run_at.is_some());

    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = rx.recv().await;
        shutdown_handle.shutdown();
    });

    let report = task.await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.state.trigger_count, 4);
    assert_eq!(report.history.len(), 1);
    assert!(report.state.next_run_at.is_some());
}

#[tokio::test]
async fn completed_at_times_state_stays_terminal_when_restored() {
    let store = Arc::new(InMemoryStateStore::new());
    let terminal_state = JobState {
        job_id: "completed-at-times".to_string(),
        trigger_count: 1,
        last_run_at: Some(Utc::now() - TimeDelta::seconds(10)),
        last_success_at: Some(Utc::now() - TimeDelta::seconds(9)),
        next_run_at: None,
        last_error: None,
    };
    store.save(&terminal_state).await.unwrap();

    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let report = scheduler
        .run(Job::without_deps(
            "completed-at-times",
            Schedule::AtTimes(vec![shanghai_after(200)]),
            Task::from_async(move |_| {
                let seen = seen.clone();
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }),
        ))
        .await
        .unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    assert_eq!(report.state, terminal_state);
}

#[tokio::test]
async fn exhausted_interval_state_stays_terminal_when_restored() {
    let store = Arc::new(InMemoryStateStore::new());
    let terminal_state = JobState {
        job_id: "exhausted-interval".to_string(),
        trigger_count: 2,
        last_run_at: Some(Utc::now() - TimeDelta::seconds(10)),
        last_success_at: Some(Utc::now() - TimeDelta::seconds(9)),
        next_run_at: None,
        last_error: None,
    };
    store.save(&terminal_state).await.unwrap();

    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let report = scheduler
        .run(
            Job::without_deps(
                "exhausted-interval",
                Schedule::Interval(Duration::from_millis(20)),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }),
            )
            .with_max_runs(2),
        )
        .await
        .unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    assert_eq!(report.state, terminal_state);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_interval_state_with_missing_next_run_at_is_repaired() {
    let store = Arc::new(InMemoryStateStore::new());
    store
        .save(&JobState {
            job_id: "repair-finite-interval".to_string(),
            trigger_count: 1,
            last_run_at: Some(Utc::now() - TimeDelta::seconds(10)),
            last_success_at: Some(Utc::now() - TimeDelta::seconds(9)),
            next_run_at: None,
            last_error: Some("stale".to_string()),
        })
        .await
        .unwrap();

    let scheduler = Arc::new(Scheduler::new(SchedulerConfig::default(), store.clone()));
    let handle = scheduler.handle();
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let (tx, mut rx) = mpsc::channel::<()>(1);

    let task = {
        let scheduler = scheduler.clone();
        tokio::spawn(async move {
            scheduler
                .run(
                    Job::without_deps(
                        "repair-finite-interval",
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
                    )
                    .with_max_runs(3),
                )
                .await
                .unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(20)).await;
    let repaired_state = store.load("repair-finite-interval").await.unwrap().unwrap();
    assert_eq!(repaired_state.trigger_count, 1);
    assert!(repaired_state.next_run_at.is_some());

    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = rx.recv().await;
        shutdown_handle.shutdown();
    });

    let report = task.await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.state.trigger_count, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_stops_new_runs_until_resumed_and_emits_events() {
    let observer = RecordingObserver::default();
    let scheduler = Arc::new(Scheduler::with_observer(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        observer.clone(),
    ));
    let handle = scheduler.handle();
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let task = {
        let scheduler = scheduler.clone();
        tokio::spawn(async move {
            scheduler
                .run(
                    Job::without_deps(
                        "pause-resume-legacy",
                        Schedule::Interval(Duration::from_millis(30)),
                        Task::from_async(move |_| {
                            let seen = seen.clone();
                            async move {
                                seen.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            }
                        }),
                    )
                    .with_max_runs(2),
                )
                .await
                .unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(10)).await;
    handle.pause().await.unwrap();
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert_eq!(invocations.load(Ordering::SeqCst), 0);

    handle.resume().await.unwrap();
    let report = task.await.unwrap();
    let events = observer.snapshot();

    assert!(!report.history.is_empty());
    assert!(invocations.load(Ordering::SeqCst) >= 1);
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::SchedulerPaused { job_id, scope, .. }
                if job_id == "pause-resume-legacy" && *scope == PauseScope::Local
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::SchedulerResumed { job_id, scope, .. }
                if job_id == "pause-resume-legacy" && *scope == PauseScope::Local
        )
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_does_not_interrupt_running_task() {
    let scheduler = Arc::new(Scheduler::new(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
    ));
    let handle = scheduler.handle();
    let (started_tx, mut started_rx) = mpsc::channel::<()>(1);
    let (finished_tx, mut finished_rx) = mpsc::channel::<()>(1);
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let task = {
        let scheduler = scheduler.clone();
        tokio::spawn(async move {
            scheduler
                .run(
                    Job::without_deps(
                        "pause-running-legacy",
                        Schedule::Interval(Duration::from_millis(10)),
                        Task::from_async(move |_| {
                            let started_tx = started_tx.clone();
                            let finished_tx = finished_tx.clone();
                            let seen = seen.clone();
                            async move {
                                seen.fetch_add(1, Ordering::SeqCst);
                                let _ = started_tx.send(()).await;
                                tokio::time::sleep(Duration::from_millis(80)).await;
                                let _ = finished_tx.send(()).await;
                                Ok(())
                            }
                        }),
                    )
                    .with_max_runs(1),
                )
                .await
                .unwrap()
        })
    };

    let _ = started_rx.recv().await;
    handle.pause().await.unwrap();
    let _ = finished_rx.recv().await;
    handle.shutdown();
    let report = task.await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert_eq!(report.state.trigger_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_trigger_runs_immediately_when_idle() {
    let scheduler = Arc::new(Scheduler::new(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
    ));
    let handle = scheduler.handle();
    let seen = Arc::new(AtomicUsize::new(0));

    let (ready_tx, ready_rx) = oneshot::channel();
    let task = {
        let scheduler = scheduler.clone();
        let seen = seen.clone();
        tokio::spawn(async move {
            let _ = ready_tx.send(());
            scheduler
                .run(
                    Job::without_deps(
                        "manual-idle",
                        Schedule::Interval(Duration::from_secs(60)),
                        Task::from_async_with_trigger(move |context: TriggeredTaskContext<()>| {
                            let seen = seen.clone();
                            async move {
                                assert_eq!(context.source, TriggerSource::Manual);
                                seen.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            }
                        }),
                    )
                    .with_max_runs(1),
                )
                .await
                .unwrap()
        })
    };

    let _ = ready_rx.await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    handle.trigger_now().await.unwrap();
    let report = task.await.unwrap();

    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert_eq!(report.state.trigger_count, 1);
    assert!(report.state.next_run_at.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_trigger_paused_until_resume() {
    let observer = RecordingObserver::default();
    let scheduler = Arc::new(Scheduler::with_observer(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        observer.clone(),
    ));
    let handle = scheduler.handle();
    let seen = Arc::new(AtomicUsize::new(0));
    let scheduled_at = shanghai_after(60_000);

    let (ready_tx, ready_rx) = oneshot::channel();
    let task = {
        let scheduler = scheduler.clone();
        let seen = seen.clone();
        tokio::spawn(async move {
            let _ = ready_tx.send(());
            scheduler
                .run(
                    Job::without_deps(
                        "manual-paused",
                        Schedule::AtTimes(vec![scheduled_at]),
                        Task::from_async_with_trigger(move |context: TriggeredTaskContext<()>| {
                            let seen = seen.clone();
                            async move {
                                assert_eq!(context.source, TriggerSource::Manual);
                                seen.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            }
                        }),
                    )
                    .with_max_runs(1),
                )
                .await
                .unwrap()
        })
    };

    let _ = ready_rx.await;
    for _ in 0..50 {
        if observer.snapshot().iter().any(|event| {
            matches!(
                event,
                SchedulerEvent::StateLoaded { job_id, .. } if job_id == "manual-paused"
            )
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.pause().await.unwrap();
    for _ in 0..50 {
        if observer.snapshot().iter().any(|event| {
            matches!(
                event,
                SchedulerEvent::SchedulerPaused { job_id, scope, .. }
                    if job_id == "manual-paused" && *scope == PauseScope::Local
            )
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.trigger_now().await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(seen.load(Ordering::SeqCst), 0);

    handle.resume().await.unwrap();
    let report = task.await.unwrap();

    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_trigger_respects_overlap_policies_while_running() {
    for policy in [
        scheduler::OverlapPolicy::Forbid,
        scheduler::OverlapPolicy::QueueOne,
        scheduler::OverlapPolicy::AllowParallel,
    ] {
        let scheduler = Arc::new(Scheduler::new(
            SchedulerConfig::default(),
            InMemoryStateStore::new(),
        ));
        let handle = scheduler.handle();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        let (ready_tx, ready_rx) = oneshot::channel();
        let task = {
            let scheduler = scheduler.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            tokio::spawn(async move {
                let _ = ready_tx.send(());
                scheduler
                    .run(
                        Job::without_deps(
                            "manual-overlap",
                            Schedule::Interval(Duration::from_secs(60)),
                            Task::from_async_with_trigger(move |context: TriggeredTaskContext<()>| {
                                let active = active.clone();
                                let max_active = max_active.clone();
                                async move {
                                    assert_eq!(context.source, TriggerSource::Manual);
                                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                                    max_active.fetch_max(current, Ordering::SeqCst);
                                    tokio::time::sleep(Duration::from_millis(60)).await;
                                    active.fetch_sub(1, Ordering::SeqCst);
                                    Ok(())
                                }
                            }),
                        )
                        .with_overlap_policy(policy)
                        .with_max_runs(match policy {
                            scheduler::OverlapPolicy::Forbid => 1,
                            _ => 2,
                        }),
                    )
                    .await
                    .unwrap()
            })
        };

        let _ = ready_rx.await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        handle.trigger_now().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        handle.trigger_now().await.unwrap();
        let report = task.await.unwrap();

        match policy {
            scheduler::OverlapPolicy::Forbid => {
                assert_eq!(report.history.len(), 1);
                assert_eq!(max_active.load(Ordering::SeqCst), 1);
            }
            scheduler::OverlapPolicy::QueueOne => {
                assert_eq!(report.history.len(), 2);
                assert_eq!(max_active.load(Ordering::SeqCst), 1);
            }
            scheduler::OverlapPolicy::AllowParallel => {
                assert_eq!(report.history.len(), 2);
                assert!(max_active.load(Ordering::SeqCst) >= 2);
            }
        }
    }
}
