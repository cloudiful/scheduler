use chrono::{NaiveTime, TimeDelta, Utc, Weekday};
use chrono_tz::Asia::Shanghai;
use scheduler::{
    InMemoryStateStore, Job, JobTimeWindow, Schedule, Scheduler, SchedulerConfig, Task,
    TimeWindowSegment, WindowedIntervalSchedule,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

fn all_day_window() -> JobTimeWindow {
    JobTimeWindow {
        timezone: Some(Shanghai),
        weekdays: Vec::new(),
        segments: vec![TimeWindowSegment::new(
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
        )],
    }
}

fn upcoming_window() -> JobTimeWindow {
    let start = (Utc::now() + TimeDelta::seconds(2))
        .with_timezone(&Shanghai)
        .time();
    let end = (Utc::now() + TimeDelta::seconds(4))
        .with_timezone(&Shanghai)
        .time();

    JobTimeWindow {
        timezone: Some(Shanghai),
        weekdays: Vec::new(),
        segments: vec![TimeWindowSegment::new(start, end)],
    }
}

#[tokio::test]
async fn windowed_interval_runs_through_scheduler_public_api() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let schedule = WindowedIntervalSchedule::new(Some(Duration::from_secs(60)))
        .with_window(all_day_window(), Some(Duration::from_millis(20)));

    let report = scheduler
        .run(
            Job::without_deps(
                "windowed-fast",
                Schedule::WindowedInterval(schedule),
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

    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(report.history.len(), 2);
    assert!(report.last_skip_reason.is_none());
    assert!(report.state.next_run_at.is_none());
}

#[tokio::test]
async fn windowed_interval_all_disabled_exits_without_running() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let schedule = WindowedIntervalSchedule::new(None).with_window(all_day_window(), None);

    let report = scheduler
        .run(Job::without_deps(
            "windowed-disabled",
            Schedule::WindowedInterval(schedule),
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
    assert!(report.history.is_empty());
    assert!(report.state.next_run_at.is_none());
}

#[test]
fn market_window_example_uses_public_types() {
    let market_open = JobTimeWindow {
        timezone: Some(Shanghai),
        weekdays: vec![
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
        ],
        segments: vec![
            TimeWindowSegment::new(
                NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
            ),
            TimeWindowSegment::new(
                NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
            ),
        ],
    };

    let schedule = WindowedIntervalSchedule::new(Some(Duration::from_secs(30 * 60)))
        .with_window(market_open, Some(Duration::from_secs(30)));

    assert!(matches!(
        Schedule::WindowedInterval(schedule),
        Schedule::WindowedInterval(_)
    ));
}

#[tokio::test]
async fn grouped_interval_can_align_into_time_window() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let report = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.run(
            Job::without_deps(
                "grouped-window-align",
                Schedule::grouped_interval(Duration::from_millis(40), 2, 1),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }),
            )
            .with_time_window(upcoming_window())
            .with_time_window_alignment()
            .with_max_runs(1),
        ),
    )
    .await
    .expect("scheduler run timed out")
    .unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert!(report.last_skip_reason.is_none());
}
