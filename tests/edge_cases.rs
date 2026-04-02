mod support;

use scheduler::{
    InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, SchedulerError, Task,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use support::{RefreshDeps, shanghai_after};

#[tokio::test]
async fn blocking_task_panic_surfaces_as_task_join_error() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new(
        "blocking-panic",
        Schedule::Interval(Duration::from_millis(20)),
        RefreshDeps {
            label: "panic",
            seen: AtomicUsize::new(0),
        },
        Task::from_blocking(|_context| -> Result<(), String> { panic!("boom") }),
    )
    .with_max_runs(1);

    let error = scheduler.run(job).await.unwrap_err();
    assert!(matches!(error, SchedulerError::TaskJoin(_)));
}

#[tokio::test]
async fn at_times_waits_for_the_first_trigger() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let started = Instant::now();
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::without_deps(
        "at-times-first-trigger",
        Schedule::AtTimes(vec![shanghai_after(120)]),
        Task::from_sync(move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    );

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert!(!report.history[0].catch_up);
    assert!(started.elapsed() >= Duration::from_millis(90));
}

#[tokio::test]
async fn interval_runs_exactly_up_to_max_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::without_deps(
        "interval-count",
        Schedule::Interval(Duration::from_millis(30)),
        Task::from_sync(move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    )
    .with_max_runs(3);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 3);
    assert_eq!(report.history.len(), 3);
    assert_eq!(report.state.trigger_count, 3);
}

#[tokio::test]
async fn at_times_respects_max_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::without_deps(
        "at-times-max-runs",
        Schedule::AtTimes(vec![shanghai_after(30), shanghai_after(80)]),
        Task::from_async(move |_| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    )
    .with_max_runs(1);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert_eq!(report.state.trigger_count, 1);
    assert!(report.state.next_run_at.is_none());
}

#[tokio::test]
async fn empty_at_times_schedule_exits_without_running() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let report = scheduler
        .run(Job::without_deps(
            "empty-at-times",
            Schedule::AtTimes(Vec::new()),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap();

    assert!(report.history.is_empty());
    assert!(report.state.next_run_at.is_none());
}

#[tokio::test]
async fn zero_max_runs_exits_without_running() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let report = scheduler
        .run(
            Job::without_deps(
                "zero-max-runs",
                Schedule::Interval(Duration::from_millis(20)),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_max_runs(0),
        )
        .await
        .unwrap();

    assert!(report.history.is_empty());
    assert!(report.state.next_run_at.is_none());
}

#[tokio::test]
async fn zero_max_runs_exits_without_running_for_at_times() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let report = scheduler
        .run(
            Job::without_deps(
                "zero-max-runs-at-times",
                Schedule::AtTimes(vec![shanghai_after(20)]),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }),
            )
            .with_max_runs(0),
        )
        .await
        .unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    assert!(report.history.is_empty());
    assert!(report.state.next_run_at.is_none());
}
