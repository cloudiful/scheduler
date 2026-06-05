#[path = "support/time.rs"]
mod time_support;

use scheduler::{
    CronSchedule, InMemoryStateStore, InvalidJobKind, Job, Schedule, Scheduler, SchedulerConfig,
    SchedulerError, Task, TaskJoinErrorKind,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use time_support::shanghai_after;

#[tokio::test]
async fn blocking_task_panic_surfaces_as_task_join_error() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new(
        "blocking-panic",
        Schedule::Interval(Duration::from_millis(20)),
        (),
        Task::from_blocking(|_context| -> Result<(), String> { panic!("boom") }),
    )
    .with_max_runs(1);

    let error = scheduler.run(job).await.unwrap_err();
    assert!(matches!(
        error,
        SchedulerError::TaskJoin(ref task_error)
            if task_error.kind() == TaskJoinErrorKind::Panic
    ));
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

#[test]
fn invalid_cron_expression_has_specific_error_kind() {
    let error = scheduler::CronSchedule::parse("@hourly").unwrap_err();

    assert!(matches!(
        error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::CronExpression
    ));
}

#[tokio::test]
async fn zero_interval_has_specific_error_kind() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let error = scheduler
        .run(Job::without_deps(
            "zero-interval",
            Schedule::Interval(Duration::ZERO),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::ZeroInterval
    ));
}

#[tokio::test]
async fn zero_staggered_interval_has_specific_error_kind() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let error = scheduler
        .run(Job::without_deps(
            "zero-staggered-interval",
            Schedule::staggered_interval(Duration::ZERO),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::ZeroInterval
    ));
}

#[tokio::test]
async fn invalid_grouped_interval_parameters_have_specific_error_kind() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let zero_interval_error = scheduler
        .run(Job::without_deps(
            "zero-grouped-interval",
            Schedule::grouped_interval(Duration::ZERO, 1, 0),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap_err();

    let zero_group_error = scheduler
        .run(Job::without_deps(
            "zero-group-size",
            Schedule::grouped_interval(Duration::from_millis(20), 0, 0),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap_err();

    let out_of_range_error = scheduler
        .run(Job::without_deps(
            "out-of-range-group-member",
            Schedule::grouped_interval(Duration::from_millis(20), 3, 3),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap_err();

    assert!(matches!(
        zero_interval_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::ZeroInterval
    ));
    assert!(matches!(
        zero_group_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
    assert!(matches!(
        out_of_range_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
}

#[tokio::test]
async fn invalid_grouped_cron_parameters_have_specific_error_kind() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let cron = CronSchedule::parse("* * * * *").unwrap();

    let zero_spread_error = scheduler
        .run(Job::without_deps(
            "zero-grouped-cron-spread",
            Schedule::grouped_cron(cron.clone(), Duration::ZERO, 1, 0),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap_err();

    let zero_group_error = scheduler
        .run(Job::without_deps(
            "zero-grouped-cron-group",
            Schedule::grouped_cron(cron.clone(), Duration::from_secs(20), 0, 0),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap_err();

    let out_of_range_error = scheduler
        .run(Job::without_deps(
            "out-of-range-grouped-cron-member",
            Schedule::grouped_cron(cron.clone(), Duration::from_secs(20), 3, 3),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap_err();

    let too_wide_error = scheduler
        .run(Job::without_deps(
            "too-wide-grouped-cron-spread",
            Schedule::grouped_cron(cron, Duration::from_secs(60), 3, 0),
            Task::from_async(|_| async { Ok(()) }),
        ))
        .await
        .unwrap_err();

    assert!(matches!(
        zero_spread_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
    assert!(matches!(
        zero_group_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
    assert!(matches!(
        out_of_range_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
    assert!(matches!(
        too_wide_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
}
