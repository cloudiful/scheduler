#![cfg(feature = "valkey-store")]

#[path = "support/valkey_cleanup.rs"]
mod valkey_cleanup;
#[path = "support/valkey_runtime.rs"]
mod valkey_runtime;
#[path = "support/valkey_state_fixtures.rs"]
mod valkey_state_fixtures;

use chrono::Utc;
use scheduler::{
    CoordinatedLeaseConfig, CoordinatedPendingTrigger, CoordinatedStateStore,
    ExecutionGuardRenewal, JobState, ValkeyCoordinatedStateStore,
};
use std::time::Duration;
use valkey_cleanup::delete_matching_prefix;
use valkey_runtime::{connection, unique_id, valkey_url};
use valkey_state_fixtures::fixture_state;

fn lease_config() -> CoordinatedLeaseConfig {
    CoordinatedLeaseConfig {
        ttl: Duration::from_millis(40),
        renew_interval: Duration::from_millis(10),
    }
}

async fn cleanup(
    connection: &mut redis::aio::MultiplexedConnection,
    state_prefix: &str,
    execution_prefix: &str,
) {
    delete_matching_prefix(connection, state_prefix).await;
    delete_matching_prefix(connection, execution_prefix).await;
}

async fn new_store(
    test_name: &str,
) -> (
    String,
    String,
    ValkeyCoordinatedStateStore,
    redis::aio::MultiplexedConnection,
) {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let state_prefix = format!("scheduler:test:{test_name}:state:{}:", unique_id(test_name));
    let execution_prefix = format!("scheduler:test:{test_name}:exec:{}:", unique_id(test_name));
    let store = ValkeyCoordinatedStateStore::with_prefixes(
        &url,
        state_prefix.clone(),
        execution_prefix.clone(),
    )
    .await
    .expect("failed to connect to valkey");
    let connection = connection(&url).await;
    (state_prefix, execution_prefix, store, connection)
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn save_state_script_updates_state_when_not_inflight() {
    let (state_prefix, execution_prefix, store, mut connection) = new_store("coord-save").await;
    let job_id = unique_id("coord-save-job");
    let initial = fixture_state(&job_id);
    let mut next = initial.clone();
    next.last_error = Some("updated".to_string());

    let runtime = store
        .load_or_initialize(&job_id, initial)
        .await
        .expect("load_or_initialize failed");
    let saved = store
        .save_state(&job_id, runtime.revision, &next)
        .await
        .expect("save_state failed");

    assert!(saved);

    let loaded = store
        .load_or_initialize(&job_id, JobState::new(&job_id, None))
        .await
        .expect("reload failed");
    assert_eq!(loaded.state, next);
    assert_eq!(loaded.revision, 1);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn claim_trigger_script_claims_occurrence_in_valkey() {
    let (state_prefix, execution_prefix, store, mut connection) = new_store("coord-claim").await;
    let job_id = unique_id("coord-claim-job");
    let initial = fixture_state(&job_id);
    let trigger = CoordinatedPendingTrigger {
        scheduled_at: Utc::now(),
        catch_up: false,
        trigger_count: 1,
    };

    let runtime = store
        .load_or_initialize(&job_id, initial.clone())
        .await
        .expect("load_or_initialize failed");
    let claim = store
        .claim_trigger(
            &job_id,
            "resource",
            runtime.revision,
            trigger.clone(),
            &initial,
            lease_config(),
        )
        .await
        .expect("claim_trigger failed")
        .expect("expected claim");

    assert!(!claim.replayed);
    assert_eq!(claim.trigger, trigger);
    assert_eq!(claim.state.revision, 1);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn reclaim_inflight_script_reclaims_expired_occurrence_from_valkey() {
    let (state_prefix, execution_prefix, store, mut connection) = new_store("coord-reclaim").await;
    let job_id = unique_id("coord-reclaim-job");
    let initial = fixture_state(&job_id);
    let trigger = CoordinatedPendingTrigger {
        scheduled_at: Utc::now(),
        catch_up: false,
        trigger_count: 1,
    };
    let config = CoordinatedLeaseConfig {
        ttl: Duration::from_millis(20),
        renew_interval: Duration::from_millis(5),
    };

    let runtime = store
        .load_or_initialize(&job_id, initial.clone())
        .await
        .expect("load_or_initialize failed");
    let claim = store
        .claim_trigger(
            &job_id,
            "resource",
            runtime.revision,
            trigger.clone(),
            &initial,
            config,
        )
        .await
        .expect("claim_trigger failed")
        .expect("expected claim");
    assert!(!claim.replayed);

    tokio::time::sleep(Duration::from_millis(25)).await;

    let replay = store
        .reclaim_inflight(&job_id, "resource", config)
        .await
        .expect("reclaim_inflight failed")
        .expect("expected replay");

    assert!(replay.replayed);
    assert_eq!(replay.trigger, trigger);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn renew_lease_script_extends_live_occurrence_lease() {
    let (state_prefix, execution_prefix, store, mut connection) = new_store("coord-renew").await;
    let job_id = unique_id("coord-renew-job");
    let initial = fixture_state(&job_id);
    let trigger = CoordinatedPendingTrigger {
        scheduled_at: Utc::now(),
        catch_up: false,
        trigger_count: 1,
    };
    let config = lease_config();

    let runtime = store
        .load_or_initialize(&job_id, initial.clone())
        .await
        .expect("load_or_initialize failed");
    let claim = store
        .claim_trigger(
            &job_id,
            "resource",
            runtime.revision,
            trigger,
            &initial,
            config,
        )
        .await
        .expect("claim_trigger failed")
        .expect("expected claim");

    let renewed = store
        .renew(&claim.lease, config)
        .await
        .expect("renew failed");

    assert_eq!(renewed, ExecutionGuardRenewal::Renewed);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn complete_script_clears_inflight_and_commits_state() {
    let (state_prefix, execution_prefix, store, mut connection) = new_store("coord-complete").await;
    let job_id = unique_id("coord-complete-job");
    let initial = fixture_state(&job_id);
    let trigger = CoordinatedPendingTrigger {
        scheduled_at: Utc::now(),
        catch_up: false,
        trigger_count: 1,
    };
    let config = lease_config();

    let runtime = store
        .load_or_initialize(&job_id, initial.clone())
        .await
        .expect("load_or_initialize failed");
    let claim = store
        .claim_trigger(
            &job_id,
            "resource",
            runtime.revision,
            trigger,
            &initial,
            config,
        )
        .await
        .expect("claim_trigger failed")
        .expect("expected claim");
    let mut final_state = initial.clone();
    final_state.last_error = Some("completed".to_string());

    let completed = store
        .complete(&job_id, claim.state.revision, &claim.lease, &final_state)
        .await
        .expect("complete failed");

    assert!(completed);

    let loaded = store
        .load_or_initialize(&job_id, JobState::new(&job_id, None))
        .await
        .expect("reload failed");
    assert_eq!(loaded.state, final_state);
    assert_eq!(loaded.revision, 2);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn pause_script_marks_runtime_as_paused() {
    let (state_prefix, execution_prefix, store, mut connection) = new_store("coord-pause").await;
    let job_id = unique_id("coord-pause-job");

    store
        .load_or_initialize(&job_id, fixture_state(&job_id))
        .await
        .expect("load_or_initialize failed");

    let changed = store.pause(&job_id).await.expect("pause failed");
    let unchanged = store.pause(&job_id).await.expect("second pause failed");

    assert!(changed);
    assert!(!unchanged);

    let loaded = store
        .load_or_initialize(&job_id, JobState::new(&job_id, None))
        .await
        .expect("reload failed");
    assert!(loaded.paused);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn resume_script_unpauses_runtime() {
    let (state_prefix, execution_prefix, store, mut connection) = new_store("coord-resume").await;
    let job_id = unique_id("coord-resume-job");

    store
        .load_or_initialize(&job_id, fixture_state(&job_id))
        .await
        .expect("load_or_initialize failed");
    store.pause(&job_id).await.expect("pause failed");

    let changed = store.resume(&job_id).await.expect("resume failed");
    let unchanged = store.resume(&job_id).await.expect("second resume failed");

    assert!(changed);
    assert!(!unchanged);

    let loaded = store
        .load_or_initialize(&job_id, JobState::new(&job_id, None))
        .await
        .expect("reload failed");
    assert!(!loaded.paused);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}
