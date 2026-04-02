mod support;

use scheduler::{
    InMemoryStateStore, Job, MissedRunPolicy, OverlapPolicy, Schedule, Scheduler, SchedulerConfig,
    Task,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use support::shanghai_after;
use tokio::sync::Mutex;

#[tokio::test]
async fn replay_all_runs_missed_times_in_order_and_keeps_the_last_time() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let scheduled = Arc::new(Mutex::new(Vec::new()));
    let seen = scheduled.clone();
    let times = vec![shanghai_after(-90), shanghai_after(-30), shanghai_after(60)];
    let expected: Vec<_> = times
        .iter()
        .map(|value| value.with_timezone(&chrono::Utc))
        .collect();

    let job = Job::without_deps(
        "replay-all",
        Schedule::AtTimes(times),
        Task::from_async(move |context| {
            let seen = seen.clone();
            async move {
                seen.lock().await.push(context.run.scheduled_at);
                Ok(())
            }
        }),
    )
    .with_missed_run_policy(MissedRunPolicy::ReplayAll);

    let report = tokio::time::timeout(Duration::from_secs(2), scheduler.run(job))
        .await
        .expect("replay-all timed out")
        .unwrap();
    let recorded = scheduled.lock().await.clone();

    assert_eq!(recorded, expected);
    assert_eq!(report.history.len(), 3);
    assert_eq!(report.history.last().unwrap().scheduled_at, expected[2]);
}

#[tokio::test]
async fn skip_policy_drops_past_occurrences() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let times = vec![
        shanghai_after(-80),
        shanghai_after(-40),
        shanghai_after(200),
    ];
    let future = times[2].with_timezone(&chrono::Utc);

    let job = Job::without_deps(
        "skip-missed",
        Schedule::AtTimes(times),
        Task::from_async(move |_| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    )
    .with_missed_run_policy(MissedRunPolicy::Skip);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert_eq!(report.history[0].scheduled_at, future);
    assert_eq!(report.state.trigger_count, 3);
}

#[tokio::test]
async fn catch_up_once_replays_one_immediate_run_then_continues() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::without_deps(
        "catch-up-once",
        Schedule::AtTimes(vec![
            shanghai_after(-120),
            shanghai_after(-80),
            shanghai_after(-30),
            shanghai_after(50),
        ]),
        Task::from_sync(move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    )
    .with_missed_run_policy(MissedRunPolicy::CatchUpOnce);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(report.history.len(), 2);
    assert!(report.history[0].catch_up);
    assert!(!report.history[1].catch_up);
    assert_eq!(report.state.trigger_count, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_forbid_skips_reentry() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let current = concurrent.clone();
    let peak = max_concurrent.clone();

    let job = Job::without_deps(
        "forbid-overlap",
        Schedule::Interval(Duration::from_millis(20)),
        Task::from_async(move |_| {
            let current = current.clone();
            let peak = peak.clone();
            async move {
                let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(70)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    )
    .with_max_runs(5)
    .with_overlap_policy(OverlapPolicy::Forbid);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
    assert!(!report.history.is_empty());
    assert!(report.history.len() < report.state.trigger_count as usize);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_queue_one_keeps_only_a_single_pending_run() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let current = concurrent.clone();
    let peak = max_concurrent.clone();

    let job = Job::without_deps(
        "queue-one",
        Schedule::Interval(Duration::from_millis(20)),
        Task::from_async(move |_| {
            let seen = seen.clone();
            let current = current.clone();
            let peak = peak.clone();
            async move {
                let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(active, Ordering::SeqCst);
                let invocation = seen.fetch_add(1, Ordering::SeqCst);
                if invocation == 0 {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                }
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    )
    .with_max_runs(4)
    .with_overlap_policy(OverlapPolicy::QueueOne);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
    assert!(invocations.load(Ordering::SeqCst) > 1);
    assert!(invocations.load(Ordering::SeqCst) < 4);
    assert_eq!(report.history.len(), invocations.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlap_allow_parallel_runs_concurrently() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let current = concurrent.clone();
    let peak = max_concurrent.clone();

    let job = Job::without_deps(
        "parallel-overlap",
        Schedule::Interval(Duration::from_millis(20)),
        Task::from_async(move |_| {
            let current = current.clone();
            let peak = peak.clone();
            async move {
                let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(70)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    )
    .with_max_runs(4)
    .with_overlap_policy(OverlapPolicy::AllowParallel);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(report.history.len(), 4);
    assert!(max_concurrent.load(Ordering::SeqCst) > 1);
}
