mod support;

use chrono_tz::UTC;
use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, StateStore, Task};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use support::shanghai_after;
use tokio::sync::mpsc;

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
