#![cfg(feature = "valkey-store")]

#[path = "support/valkey_runtime.rs"]
mod valkey_runtime;
#[path = "support/valkey_state_fixtures.rs"]
mod valkey_state_fixtures;

use chrono::{TimeDelta, Utc};
use redis::AsyncCommands;
use scheduler::{Job, Schedule, Scheduler, SchedulerConfig, StateStore, Task, ValkeyStateStore};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use valkey_runtime::{connection, unique_id, valkey_url};
use valkey_state_fixtures::fixture_state;
use tokio::sync::mpsc;

const DEFAULT_KEY_PREFIX: &str = "scheduler:valkey:job-state:";
const LEGACY_DEFAULT_KEY_PREFIX: &str = "scheduler:job-state:";

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn valkey_store_round_trips_state() {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let prefix = format!("scheduler:test:valkey-store:{}:", unique_id("scheduler-it"));
    let store = ValkeyStateStore::with_prefix(&url, prefix.clone())
        .await
        .expect("failed to connect to valkey");
    let mut connection = connection(&url).await;
    let job_id = unique_id("scheduler-it");
    let key = format!("{prefix}{job_id}");
    let state = fixture_state(&job_id);

    store.save(&state).await.expect("save failed");
    let loaded = store.load(&job_id).await.expect("load failed");

    assert_eq!(loaded, Some(state));

    let _: usize = connection.del(key).await.expect("cleanup failed");
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn valkey_store_reads_legacy_default_prefix() {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let store = ValkeyStateStore::new(&url)
        .await
        .expect("failed to connect to valkey");
    let mut connection = connection(&url).await;
    let job_id = unique_id("scheduler-it");
    let legacy_key = format!("{LEGACY_DEFAULT_KEY_PREFIX}{job_id}");
    let default_key = format!("{DEFAULT_KEY_PREFIX}{job_id}");
    let state = fixture_state(&job_id);
    let payload = serde_json::to_string(&state).expect("failed to serialize job state");

    let _: () = connection
        .set(&legacy_key, payload)
        .await
        .expect("failed to seed legacy key");

    let loaded = store.load(&job_id).await.expect("load failed");

    assert_eq!(loaded, Some(state));

    let _: usize = connection
        .del(legacy_key)
        .await
        .expect("legacy cleanup failed");
    let _: usize = connection
        .del(default_key)
        .await
        .expect("default cleanup failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn scheduler_restores_state_across_instances_using_valkey() {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let prefix = format!("scheduler:test:restore:{}:", unique_id("scheduler-it"));
    let mut connection = connection(&url).await;
    let job_id = unique_id("scheduler-it");
    let key = format!("{prefix}{job_id}");
    let scheduler_one = Scheduler::new(
        SchedulerConfig::default(),
        ValkeyStateStore::with_prefix(&url, prefix.clone())
            .await
            .expect("failed to connect to valkey"),
    );
    let handle = scheduler_one.handle();
    let (tx, mut rx) = mpsc::channel::<()>(1);
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();
    let first_at = Utc::now() + TimeDelta::milliseconds(30);
    let second_at = Utc::now() + TimeDelta::milliseconds(120);
    let times = vec![
        first_at.with_timezone(&chrono_tz::Asia::Shanghai),
        second_at.with_timezone(&chrono_tz::Asia::Shanghai),
    ];

    let job = Job::without_deps(
        job_id.clone(),
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

    let first_report = scheduler_one.run(job).await.expect("first run failed");

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(first_report.history.len(), 1);

    let scheduler_two = Scheduler::new(
        SchedulerConfig::default(),
        ValkeyStateStore::with_prefix(&url, prefix.clone())
            .await
            .expect("failed to reconnect to valkey"),
    );
    let seen = invocations.clone();
    let job = Job::without_deps(
        job_id.clone(),
        Schedule::AtTimes(times),
        Task::from_async(move |_| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    );

    let second_report = scheduler_two.run(job).await.expect("second run failed");

    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(second_report.history.len(), 1);
    assert_eq!(second_report.state.trigger_count, 2);
    assert!(second_report.state.next_run_at.is_none());

    let _: usize = connection.del(key).await.expect("cleanup failed");
}
