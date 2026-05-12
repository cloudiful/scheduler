use chrono::{TimeDelta, Utc};
use scheduler::{
    InMemoryStateStore, Job, JobState, Schedule, Scheduler, SchedulerConfig, StateStore, Task,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::mpsc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grouped_interval_persists_state_after_the_first_run() {
    let store = Arc::new(InMemoryStateStore::new());
    let initial_due = Utc::now() + TimeDelta::milliseconds(30);
    store
        .save(&JobState::new("scrape-grouped", Some(initial_due)))
        .await
        .unwrap();

    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let handle = scheduler.handle();
    let (tx, mut rx) = mpsc::channel::<()>(1);
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let job = Job::without_deps(
        "scrape-grouped",
        Schedule::grouped_interval_with_seed(
            TimeDelta::days(1).to_std().unwrap(),
            3,
            1,
            "example.com",
        ),
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
    .with_max_runs(2);

    let shutdown_handle = handle.clone();
    let task = tokio::spawn(async move {
        tokio::spawn(async move {
            let _ = rx.recv().await;
            shutdown_handle.shutdown();
        });
        scheduler.run(job).await.unwrap()
    });

    let report = task.await.unwrap();

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    let first_scheduled_at = report.history[0].scheduled_at;
    let saved_state = store.load("scrape-grouped").await.unwrap().unwrap();
    assert_eq!(
        saved_state.next_run_at,
        Some(first_scheduled_at + TimeDelta::days(1))
    );
    assert_eq!(report.state.next_run_at, saved_state.next_run_at);
}
