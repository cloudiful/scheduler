#[path = "support/refresh.rs"]
mod refresh_support;
#[path = "support/time.rs"]
mod time_support;

use chrono::Utc;
use scheduler::{
    InMemoryStateStore, Job, RunContext, Schedule, Scheduler, SchedulerConfig, Task, TaskContext,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use refresh_support::RefreshDeps;
use time_support::shanghai_after;

#[tokio::test]
async fn async_task_without_context_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::without_deps(
        "async-no-context",
        Schedule::Interval(Duration::from_millis(20)),
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
}

#[tokio::test]
async fn sync_task_without_context_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::without_deps(
        "sync-no-context",
        Schedule::Interval(Duration::from_millis(20)),
        Task::from_sync(move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    )
    .with_max_runs(1);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
}

#[tokio::test]
async fn async_task_with_run_context_receives_scheduled_time() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let planned = shanghai_after(70).with_timezone(&Utc);

    let job = Job::without_deps(
        "async-with-run",
        Schedule::AtTimes(vec![planned.with_timezone(&chrono_tz::Asia::Shanghai)]),
        Task::from_async(move |context: TaskContext<()>| async move {
            let run: RunContext = context.run;
            assert_eq!(run.scheduled_at, planned);
            Ok(())
        }),
    );

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(report.history.len(), 1);
    assert_eq!(report.history[0].scheduled_at, planned);
}

#[tokio::test]
async fn async_task_with_injected_deps_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new(
        "async-with-deps",
        Schedule::Interval(Duration::from_millis(20)),
        RefreshDeps {
            label: "deps-only",
            seen: AtomicUsize::new(0),
        },
        Task::from_async(|context: TaskContext<RefreshDeps>| async move {
            assert_eq!(context.deps.label, "deps-only");
            context.deps.seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    )
    .with_max_runs(1);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(report.history.len(), 1);
    assert_eq!(report.state.trigger_count, 1);
}

#[tokio::test]
async fn async_task_with_full_context_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let planned = shanghai_after(60).with_timezone(&Utc);

    let job = Job::new(
        "async-with-context",
        Schedule::AtTimes(vec![planned.with_timezone(&chrono_tz::Asia::Shanghai)]),
        RefreshDeps {
            label: "context",
            seen: AtomicUsize::new(0),
        },
        Task::from_async(move |context: TaskContext<RefreshDeps>| async move {
            assert_eq!(context.run.scheduled_at, planned);
            assert_eq!(context.deps.label, "context");
            context.deps.seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    );

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(report.history.len(), 1);
}

#[tokio::test]
async fn blocking_task_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::without_deps(
        "blocking-task",
        Schedule::Interval(Duration::from_millis(20)),
        Task::from_blocking(move |_| {
            std::thread::sleep(Duration::from_millis(10));
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    )
    .with_max_runs(1);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
}
