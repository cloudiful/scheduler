use chrono::{TimeDelta, Timelike, Utc};
use scheduler::{
    CronSchedule, InMemoryStateStore, Job, JobState, MissedRunPolicy, OverlapPolicy, Schedule,
    Scheduler, SchedulerConfig, StateStore, Task,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};

fn minute_floor(value: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    value
        .with_second(0)
        .expect("valid second")
        .with_nanosecond(0)
        .expect("valid nanosecond")
}

#[tokio::test]
async fn cron_replay_all_runs_each_missed_minute_in_order() {
    let store = Arc::new(InMemoryStateStore::new());
    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let seen = recorded.clone();
    let now_floor = minute_floor(Utc::now());
    let first_due = now_floor - TimeDelta::minutes(2);
    let second_due = now_floor - TimeDelta::minutes(1);
    let expected = vec![first_due, second_due, now_floor];

    store
        .save(&JobState::new("cron-replay-all", Some(first_due)))
        .await
        .unwrap();

    let job = Job::without_deps(
        "cron-replay-all",
        Schedule::Cron(CronSchedule::parse("* * * * *").unwrap()),
        Task::from_async(move |context| {
            let seen = seen.clone();
            async move {
                seen.lock().await.push(context.run.scheduled_at);
                Ok(())
            }
        }),
    )
    .with_missed_run_policy(MissedRunPolicy::ReplayAll)
    .with_max_runs(3);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(*recorded.lock().await, expected);
    assert_eq!(report.history.len(), 3);
    assert_eq!(report.state.trigger_count, 3);
    assert!(report.state.next_run_at.is_none());
}

#[tokio::test]
async fn cron_catch_up_once_replays_only_the_latest_due_minute() {
    let store = Arc::new(InMemoryStateStore::new());
    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let now_floor = minute_floor(Utc::now());
    let first_due = now_floor - TimeDelta::minutes(2);

    store
        .save(&JobState::new("cron-catch-up-once", Some(first_due)))
        .await
        .unwrap();

    let job = Job::without_deps(
        "cron-catch-up-once",
        Schedule::Cron(CronSchedule::parse("* * * * *").unwrap()),
        Task::from_async(move |_| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    )
    .with_missed_run_policy(MissedRunPolicy::CatchUpOnce)
    .with_max_runs(3);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert_eq!(report.history[0].scheduled_at, now_floor);
    assert!(report.history[0].catch_up);
    assert_eq!(report.state.trigger_count, 3);
    assert!(report.state.next_run_at.is_none());
}

#[tokio::test]
async fn cron_skip_advances_state_then_waits_for_the_next_future_minute() {
    let store = Arc::new(InMemoryStateStore::new());
    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let handle = scheduler.handle();
    let now_floor = minute_floor(Utc::now());
    let first_due = now_floor - TimeDelta::minutes(2);
    let expected_next = now_floor + TimeDelta::minutes(1);

    store
        .save(&JobState::new("cron-skip", Some(first_due)))
        .await
        .unwrap();

    let task = tokio::spawn(async move {
        scheduler
            .run(
                Job::without_deps(
                    "cron-skip",
                    Schedule::Cron(CronSchedule::parse("* * * * *").unwrap()),
                    Task::from_async(|_| async { Ok(()) }),
                )
                .with_missed_run_policy(MissedRunPolicy::Skip),
            )
            .await
            .unwrap()
    });

    tokio::time::sleep(Duration::from_millis(40)).await;
    handle.cancel();

    let report = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("scheduler task timed out")
        .unwrap();

    assert!(report.history.is_empty());
    assert_eq!(report.state.trigger_count, 3);
    assert_eq!(report.state.next_run_at, Some(expected_next));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cron_overlap_allow_parallel_runs_due_replay_concurrently() {
    let store = Arc::new(InMemoryStateStore::new());
    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let current = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let active = current.clone();
    let max_seen = peak.clone();
    let now_floor = minute_floor(Utc::now());
    let first_due = now_floor - TimeDelta::minutes(3);

    store
        .save(&JobState::new("cron-parallel", Some(first_due)))
        .await
        .unwrap();

    let job = Job::without_deps(
        "cron-parallel",
        Schedule::Cron(CronSchedule::parse("* * * * *").unwrap()),
        Task::from_async(move |_| {
            let active = active.clone();
            let max_seen = max_seen.clone();
            async move {
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(count, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(40)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    )
    .with_missed_run_policy(MissedRunPolicy::ReplayAll)
    .with_overlap_policy(OverlapPolicy::AllowParallel)
    .with_max_runs(4);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(report.history.len(), 4);
    assert!(peak.load(Ordering::SeqCst) > 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cron_state_is_restored_across_scheduler_instances() {
    let store = Arc::new(InMemoryStateStore::new());
    let scheduler_one = Scheduler::new(SchedulerConfig::default(), store.clone());
    let handle = scheduler_one.handle();
    let (tx, mut rx) = mpsc::channel::<()>(1);
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let now_floor = minute_floor(Utc::now());
    let first_due = now_floor - TimeDelta::minutes(1);

    store
        .save(&JobState::new("cron-restore", Some(first_due)))
        .await
        .unwrap();

    let job = Job::without_deps(
        "cron-restore",
        Schedule::Cron(CronSchedule::parse("* * * * *").unwrap()),
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
    )
    .with_missed_run_policy(MissedRunPolicy::ReplayAll)
    .with_max_runs(2);

    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = rx.recv().await;
        shutdown_handle.shutdown();
    });

    let first_report = scheduler_one.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(first_report.history.len(), 1);

    let saved_state = store.load("cron-restore").await.unwrap().unwrap();
    assert_eq!(saved_state.next_run_at, Some(now_floor));

    let scheduler_two = Scheduler::new(SchedulerConfig::default(), store.clone());
    let seen = invocations.clone();
    let job = Job::without_deps(
        "cron-restore",
        Schedule::Cron(CronSchedule::parse("* * * * *").unwrap()),
        Task::from_async(move |_| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    )
    .with_missed_run_policy(MissedRunPolicy::ReplayAll)
    .with_max_runs(2);

    let second_report = scheduler_two.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(second_report.history.len(), 1);
    assert_eq!(second_report.state.trigger_count, 2);
    assert!(second_report.state.next_run_at.is_none());
}
