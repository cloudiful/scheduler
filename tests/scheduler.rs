use chrono::{TimeDelta, Utc};
use chrono_tz::{Asia::Shanghai, UTC};
use scheduler::{
    InMemoryStateStore, Job, MissedRunPolicy, OverlapPolicy, RunContext, Schedule, Scheduler,
    SchedulerConfig, SchedulerError, StateStore, TaskContext,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};

fn shanghai_after(milliseconds: i64) -> chrono::DateTime<chrono_tz::Tz> {
    Utc::now().with_timezone(&Shanghai) + TimeDelta::milliseconds(milliseconds)
}

#[derive(Debug)]
struct RefreshDeps {
    label: &'static str,
    seen: AtomicUsize,
}

#[tokio::test]
async fn async_task_without_context_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::new(
        "async-no-context",
        Schedule::Interval(Duration::from_millis(20)),
        move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
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

    let job = Job::new_sync(
        "sync-no-context",
        Schedule::Interval(Duration::from_millis(20)),
        move || {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
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

    let job = Job::new_with_run(
        "async-with-run",
        Schedule::AtTimes(vec![planned.with_timezone(&Shanghai)]),
        move |context: RunContext| async move {
            assert_eq!(context.scheduled_at, planned);
            Ok(())
        },
    );

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(report.history.len(), 1);
    assert_eq!(report.history[0].scheduled_at, planned);
}

#[tokio::test]
async fn async_task_with_injected_deps_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new_with(
        "async-with-deps",
        Schedule::Interval(Duration::from_millis(20)),
        RefreshDeps {
            label: "deps-only",
            seen: AtomicUsize::new(0),
        },
        |deps: Arc<RefreshDeps>| async move {
            assert_eq!(deps.label, "deps-only");
            deps.seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
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

    let job = Job::new_with_context(
        "async-with-context",
        Schedule::AtTimes(vec![planned.with_timezone(&Shanghai)]),
        RefreshDeps {
            label: "context",
            seen: AtomicUsize::new(0),
        },
        move |context: TaskContext<RefreshDeps>| async move {
            assert_eq!(context.run.scheduled_at, planned);
            assert_eq!(context.deps.label, "context");
            context.deps.seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    );

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(report.history.len(), 1);
}

#[tokio::test]
async fn blocking_task_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::new_blocking(
        "blocking-task",
        Schedule::Interval(Duration::from_millis(20)),
        move || {
            std::thread::sleep(Duration::from_millis(10));
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .with_max_runs(1);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
}

#[tokio::test]
async fn blocking_task_panic_surfaces_as_task_join_error() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new_blocking_with_context(
        "blocking-panic",
        Schedule::Interval(Duration::from_millis(20)),
        RefreshDeps {
            label: "panic",
            seen: AtomicUsize::new(0),
        },
        |_context: TaskContext<RefreshDeps>| -> Result<(), String> {
            panic!("boom");
        },
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

    let job = Job::new_sync(
        "at-times-first-trigger",
        Schedule::AtTimes(vec![shanghai_after(120)]),
        move || {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    );

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert!(!report.history[0].catch_up);
    assert!(started.elapsed() >= Duration::from_millis(90));
}

#[tokio::test]
async fn replay_all_runs_missed_times_in_order_and_keeps_the_last_time() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let scheduled = Arc::new(Mutex::new(Vec::new()));
    let seen = scheduled.clone();
    let times = vec![shanghai_after(-90), shanghai_after(-30), shanghai_after(60)];
    let expected: Vec<_> = times
        .iter()
        .map(|value| value.with_timezone(&Utc))
        .collect();

    let job = Job::new_with_run("replay-all", Schedule::AtTimes(times), move |context| {
        let seen = seen.clone();
        async move {
            seen.lock().await.push(context.scheduled_at);
            Ok(())
        }
    })
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
    let future = times[2].with_timezone(&Utc);

    let job = Job::new("skip-missed", Schedule::AtTimes(times), move || {
        let seen = seen.clone();
        async move {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
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

    let job = Job::new_sync(
        "catch-up-once",
        Schedule::AtTimes(vec![
            shanghai_after(-120),
            shanghai_after(-80),
            shanghai_after(-30),
            shanghai_after(50),
        ]),
        move || {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .with_missed_run_policy(MissedRunPolicy::CatchUpOnce);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(report.history.len(), 2);
    assert!(report.history[0].catch_up);
    assert!(!report.history[1].catch_up);
    assert_eq!(report.state.trigger_count, 4);
}

#[tokio::test]
async fn interval_runs_exactly_up_to_max_runs() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::new_sync(
        "interval-count",
        Schedule::Interval(Duration::from_millis(30)),
        move || {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
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

    let job = Job::new(
        "at-times-max-runs",
        Schedule::AtTimes(vec![shanghai_after(30), shanghai_after(80)]),
        move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
    )
    .with_max_runs(1);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert_eq!(report.state.trigger_count, 1);
    assert!(report.state.next_run_at.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlap_forbid_skips_reentry() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let current = concurrent.clone();
    let peak = max_concurrent.clone();

    let job = Job::new(
        "forbid-overlap",
        Schedule::Interval(Duration::from_millis(20)),
        move || {
            let current = current.clone();
            let peak = peak.clone();
            async move {
                let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(70)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        },
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

    let job = Job::new(
        "queue-one",
        Schedule::Interval(Duration::from_millis(20)),
        move || {
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
        },
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

    let job = Job::new(
        "parallel-overlap",
        Schedule::Interval(Duration::from_millis(20)),
        move || {
            let current = current.clone();
            let peak = peak.clone();
            async move {
                let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(70)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        },
    )
    .with_max_runs(4)
    .with_overlap_policy(OverlapPolicy::AllowParallel);

    let report = scheduler.run(job).await.unwrap();

    assert_eq!(report.history.len(), 4);
    assert!(max_concurrent.load(Ordering::SeqCst) > 1);
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

    let job = Job::new(
        "restore-state",
        Schedule::AtTimes(times.clone()),
        move || {
            let tx = tx.clone();
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                let _ = tx.send(()).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }
        },
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
    let job = Job::new("restore-state", Schedule::AtTimes(times), move || {
        let seen = seen.clone();
        async move {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });

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
    let planned_utc = planned.with_timezone(&Utc);

    let job = Job::new_with_run(
        "timezone-explicit",
        Schedule::AtTimes(vec![planned]),
        move |context| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                assert_eq!(context.scheduled_at, planned_utc);
                Ok(())
            }
        },
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
                Job::new(
                    "cancel-waiting",
                    Schedule::Interval(Duration::from_millis(200)),
                    || async { Ok(()) },
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

    let job = Job::new(
        "shutdown-running",
        Schedule::Interval(Duration::from_millis(10)),
        move || {
            let tx = tx.clone();
            async move {
                let _ = tx.send(()).await;
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok(())
            }
        },
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

#[tokio::test]
async fn empty_at_times_schedule_exits_without_running() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let report = scheduler
        .run(Job::new(
            "empty-at-times",
            Schedule::AtTimes(Vec::new()),
            || async { Ok(()) },
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
            Job::new(
                "zero-max-runs",
                Schedule::Interval(Duration::from_millis(20)),
                || async { Ok(()) },
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
            Job::new(
                "zero-max-runs-at-times",
                Schedule::AtTimes(vec![shanghai_after(20)]),
                move || {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
            .with_max_runs(0),
        )
        .await
        .unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    assert!(report.history.is_empty());
    assert!(report.state.next_run_at.is_none());
}
