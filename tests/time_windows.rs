#[path = "support/time.rs"]
mod time_support;

use chrono::{Datelike, NaiveTime, Timelike, Utc, Weekday};
use chrono_tz::Asia::Shanghai;
use scheduler::{
    InMemoryStateStore, Job, JobTimeWindow, OverlapPolicy, RunSkipReason, Schedule, Scheduler,
    SchedulerConfig, SchedulerEvent, SchedulerObserver, Task, TimeWindowSegment,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use time_support::shanghai_after;

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

fn next_weekday(weekday: Weekday) -> Weekday {
    match weekday {
        Weekday::Mon => Weekday::Tue,
        Weekday::Tue => Weekday::Wed,
        Weekday::Wed => Weekday::Thu,
        Weekday::Thu => Weekday::Fri,
        Weekday::Fri => Weekday::Sat,
        Weekday::Sat => Weekday::Sun,
        Weekday::Sun => Weekday::Mon,
    }
}

fn shift_time(time: chrono::NaiveTime, delta_seconds: i64) -> chrono::NaiveTime {
    let seconds = time.num_seconds_from_midnight() as i64 + delta_seconds;
    let seconds = seconds.rem_euclid(24 * 60 * 60) as u32;
    NaiveTime::from_num_seconds_from_midnight_opt(seconds, time.nanosecond()).unwrap()
}

#[tokio::test]
async fn job_without_window_keeps_existing_behavior() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let report = scheduler
        .run(
            Job::without_deps(
                "no-window",
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
    assert!(report.last_skip_reason.is_none());
}

#[tokio::test]
async fn inside_window_runs_normally_with_scheduler_timezone_fallback() {
    let local_now = Utc::now().with_timezone(&Shanghai);
    let observer = RecordingObserver::default();
    let scheduler = Scheduler::with_observer(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        observer.clone(),
    );
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let report = scheduler
        .run(
            Job::without_deps(
                "inside-window",
                Schedule::AtTimes(vec![shanghai_after(20)]),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }),
            )
            .with_time_window(JobTimeWindow {
                timezone: None,
                weekdays: vec![local_now.weekday()],
                segments: vec![],
            }),
        )
        .await
        .unwrap();

    let events = observer.snapshot();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert!(report.last_skip_reason.is_none());
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SchedulerEvent::RunSkipped { .. }))
    );
}

#[tokio::test]
async fn outside_window_skips_run_and_records_reason() {
    let local_now = Utc::now().with_timezone(&Shanghai);
    let observer = RecordingObserver::default();
    let scheduler = Scheduler::with_observer(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        observer.clone(),
    );
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let window = JobTimeWindow {
        timezone: None,
        weekdays: vec![next_weekday(local_now.weekday())],
        segments: vec![],
    };

    let report = scheduler
        .run(
            Job::without_deps(
                "outside-window",
                Schedule::AtTimes(vec![shanghai_after(20)]),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }),
            )
            .with_time_window(window),
        )
        .await
        .unwrap();

    let events = observer.snapshot();

    assert_eq!(invocations.load(Ordering::SeqCst), 0);
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
        } if job_id == "outside-window" && *reason == RunSkipReason::OutsideTimeWindow
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_trigger_is_rechecked_before_starting() {
    let local_now = Utc::now().with_timezone(&Shanghai);
    let observer = RecordingObserver::default();
    let scheduler = Scheduler::with_observer(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        observer.clone(),
    );
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let window = JobTimeWindow {
        timezone: Some(Shanghai),
        weekdays: vec![local_now.weekday()],
        segments: vec![TimeWindowSegment::new(
            shift_time(local_now.time(), -1),
            shift_time(local_now.time(), 2),
        )],
    };

    let report = tokio::time::timeout(
        Duration::from_secs(6),
        scheduler.run(
            Job::without_deps(
                "queued-window-skip",
                Schedule::AtTimes(vec![shanghai_after(100), shanghai_after(200)]),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    async move {
                        let invocation = seen.fetch_add(1, Ordering::SeqCst);
                        if invocation == 0 {
                            tokio::time::sleep(Duration::from_millis(2500)).await;
                        }
                        Ok(())
                    }
                }),
            )
            .with_overlap_policy(OverlapPolicy::QueueOne)
            .with_time_window(window)
            .with_max_runs(2),
        ),
    )
    .await
    .expect("scheduler run timed out")
    .unwrap();

    let events = observer.snapshot();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
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
        } if job_id == "queued-window-skip" && *reason == RunSkipReason::OutsideTimeWindow
    )));
}
