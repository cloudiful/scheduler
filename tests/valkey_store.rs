#![cfg(feature = "valkey-store")]

use chrono::{TimeDelta, Utc};
use redis::{AsyncCommands, Client};
use scheduler::{
    Job, JobState, Schedule, Scheduler, SchedulerConfig, StateStore, Task, ValkeyStateStore,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

const DEFAULT_KEY_PREFIX: &str = "scheduler:valkey:job-state:";
const LEGACY_DEFAULT_KEY_PREFIX: &str = "scheduler:job-state:";

fn valkey_url() -> Option<String> {
    std::env::var("SCHEDULER_VALKEY_URL").ok()
}

fn unique_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    format!("scheduler-it-{}-{now}", std::process::id())
}

fn fixture_state(job_id: &str) -> JobState {
    JobState {
        job_id: job_id.to_string(),
        trigger_count: 3,
        last_run_at: Some(Utc::now()),
        last_success_at: Some(Utc::now() + TimeDelta::seconds(1)),
        next_run_at: Some(Utc::now() + TimeDelta::seconds(10)),
        last_error: Some("integration".to_string()),
    }
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn valkey_store_round_trips_state() {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let prefix = format!("scheduler:test:valkey-store:{}:", unique_id());
    let store = ValkeyStateStore::with_prefix(&url, prefix.clone())
        .await
        .expect("failed to connect to valkey");
    let client = Client::open(url).expect("invalid valkey url");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to get valkey connection");
    let job_id = unique_id();
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
    let client = Client::open(url).expect("invalid valkey url");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to get valkey connection");
    let job_id = unique_id();
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
    let prefix = format!("scheduler:test:restore:{}:", unique_id());
    let client = Client::open(url.clone()).expect("invalid valkey url");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to get valkey connection");
    let job_id = unique_id();
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
