#![cfg(feature = "valkey-store")]

#[path = "support/valkey_cleanup.rs"]
mod valkey_cleanup;
#[path = "support/valkey_runtime.rs"]
mod valkey_runtime;
#[path = "support/valkey_state_fixtures.rs"]
mod valkey_state_fixtures;

use chrono::Utc;
use scheduler::{
    CoordinatedClaim, CoordinatedCompletion, CoordinatedLeaseConfig, CoordinatedPendingTrigger,
    CoordinatedRuntimeState, CoordinatedStateStore, ExecutionGuardRenewal, ExecutionGuardScope,
    JobState, TriggerSource, ValkeyCoordinatedStateStore,
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

async fn load_runtime(
    store: &ValkeyCoordinatedStateStore,
    job_id: &str,
    state: JobState,
) -> CoordinatedRuntimeState {
    store
        .load_or_initialize(job_id, state)
        .await
        .expect("load_or_initialize failed")
}

async fn claim_occurrence(
    store: &ValkeyCoordinatedStateStore,
    job_id: &str,
    revision: u64,
    state: &JobState,
    scheduled_at: chrono::DateTime<Utc>,
    trigger_count: u32,
    config: CoordinatedLeaseConfig,
) -> CoordinatedClaim {
    claim_with_scope(
        store,
        job_id,
        revision,
        state,
        scheduled_at,
        trigger_count,
        config,
        ExecutionGuardScope::Occurrence,
    )
    .await
}

async fn claim_resource(
    store: &ValkeyCoordinatedStateStore,
    job_id: &str,
    revision: u64,
    state: &JobState,
    scheduled_at: chrono::DateTime<Utc>,
    trigger_count: u32,
    config: CoordinatedLeaseConfig,
) -> CoordinatedClaim {
    claim_with_scope(
        store,
        job_id,
        revision,
        state,
        scheduled_at,
        trigger_count,
        config,
        ExecutionGuardScope::Resource,
    )
    .await
}

async fn claim_with_scope(
    store: &ValkeyCoordinatedStateStore,
    job_id: &str,
    revision: u64,
    state: &JobState,
    scheduled_at: chrono::DateTime<Utc>,
    trigger_count: u32,
    config: CoordinatedLeaseConfig,
    scope: ExecutionGuardScope,
) -> CoordinatedClaim {
    store
        .claim_trigger(
            job_id,
            "resource",
            revision,
            CoordinatedPendingTrigger {
                scheduled_at,
                catch_up: false,
                trigger_count,
                source: TriggerSource::Scheduled,
            },
            state,
            config,
            scope,
        )
        .await
        .expect("claim_trigger failed")
        .expect("expected claim")
}

async fn complete_claim(
    store: &ValkeyCoordinatedStateStore,
    job_id: &str,
    claim: &CoordinatedClaim,
    completion: CoordinatedCompletion,
) -> bool {
    store
        .complete(job_id, &claim.lease, completion)
        .await
        .expect("complete failed")
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn save_state_script_updates_state_when_not_inflight() {
    let (state_prefix, execution_prefix, store, mut connection) = new_store("coord-save").await;
    let job_id = unique_id("coord-save-job");
    let initial = fixture_state(&job_id);
    let mut next = initial.clone();
    next.last_error = Some("updated".to_string());

    let runtime = load_runtime(&store, &job_id, initial).await;
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
            source: TriggerSource::Scheduled,
    };

    let runtime = load_runtime(&store, &job_id, initial.clone()).await;
    let claim = claim_occurrence(
        &store,
        &job_id,
        runtime.revision,
        &initial,
        trigger.scheduled_at,
        trigger.trigger_count,
        lease_config(),
    )
    .await;

    assert!(!claim.replayed);
    assert_eq!(claim.trigger, trigger);
    assert_eq!(claim.state.revision, 1);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn claim_trigger_script_allows_parallel_occurrences() {
    let (state_prefix, execution_prefix, store, mut connection) =
        new_store("coord-parallel-claim").await;
    let job_id = unique_id("coord-parallel-claim-job");
    let mut state = fixture_state(&job_id);
    let first = Utc::now();
    let second = first + chrono::TimeDelta::milliseconds(10);

    let runtime = load_runtime(&store, &job_id, state.clone()).await;
    let first_claim = claim_occurrence(
        &store,
        &job_id,
        runtime.revision,
        &state,
        first,
        1,
        lease_config(),
    )
    .await;

    state.trigger_count = 2;
    let second_claim = claim_occurrence(
        &store,
        &job_id,
        first_claim.state.revision,
        &state,
        second,
        2,
        lease_config(),
    )
    .await;

    assert_ne!(first_claim.lease.lease_key, second_claim.lease.lease_key);
    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn claim_trigger_script_resource_scope_blocks_other_claims() {
    let (state_prefix, execution_prefix, store, mut connection) =
        new_store("coord-resource-claim").await;
    let job_id = unique_id("coord-resource-claim-job");
    let mut state = fixture_state(&job_id);
    let first = Utc::now();
    let second = first + chrono::TimeDelta::milliseconds(10);

    let runtime = load_runtime(&store, &job_id, state.clone()).await;
    let first_claim = claim_resource(
        &store,
        &job_id,
        runtime.revision,
        &state,
        first,
        1,
        lease_config(),
    )
    .await;

    state.trigger_count = 2;
    let blocked = store
        .claim_trigger(
            &job_id,
            "resource",
            first_claim.state.revision,
            CoordinatedPendingTrigger {
                scheduled_at: second,
                catch_up: false,
                trigger_count: 2,
            source: TriggerSource::Scheduled,
            },
            &state,
            lease_config(),
            ExecutionGuardScope::Occurrence,
        )
        .await
        .expect("occurrence claim failed");

    assert!(blocked.is_none());
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
            source: TriggerSource::Scheduled,
    };
    let config = CoordinatedLeaseConfig {
        ttl: Duration::from_millis(20),
        renew_interval: Duration::from_millis(5),
    };

    let runtime = load_runtime(&store, &job_id, initial.clone()).await;
    let claim = claim_occurrence(
        &store,
        &job_id,
        runtime.revision,
        &initial,
        trigger.scheduled_at,
        trigger.trigger_count,
        config,
    )
    .await;
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
            source: TriggerSource::Scheduled,
    };
    let config = lease_config();

    let runtime = load_runtime(&store, &job_id, initial.clone()).await;
    let claim = claim_occurrence(
        &store,
        &job_id,
        runtime.revision,
        &initial,
        trigger.scheduled_at,
        trigger.trigger_count,
        config,
    )
    .await;

    let renewed = store
        .renew(&claim.lease, config)
        .await
        .expect("renew failed");

    assert_eq!(renewed, ExecutionGuardRenewal::Renewed);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn complete_script_applies_completion_patch_and_clears_inflight() {
    let (state_prefix, execution_prefix, store, mut connection) = new_store("coord-complete").await;
    let job_id = unique_id("coord-complete-job");
    let initial = fixture_state(&job_id);
    let trigger = CoordinatedPendingTrigger {
        scheduled_at: Utc::now(),
        catch_up: false,
        trigger_count: 1,
            source: TriggerSource::Scheduled,
    };
    let config = lease_config();

    let runtime = load_runtime(&store, &job_id, initial.clone()).await;
    let claim = claim_occurrence(
        &store,
        &job_id,
        runtime.revision,
        &initial,
        trigger.scheduled_at,
        trigger.trigger_count,
        config,
    )
    .await;
    let completed = complete_claim(
        &store,
        &job_id,
        &claim,
        CoordinatedCompletion {
            last_run_at: Utc::now(),
            last_success_at: None,
            last_error: Some("completed".to_string()),
        },
    )
    .await;

    assert!(completed);

    let loaded = store
        .load_or_initialize(&job_id, JobState::new(&job_id, None))
        .await
        .expect("reload failed");
    assert_eq!(loaded.state.last_error, Some("completed".to_string()));
    assert_eq!(loaded.revision, 2);

    cleanup(&mut connection, &state_prefix, &execution_prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn complete_script_removes_only_matching_inflight() {
    let (state_prefix, execution_prefix, store, mut connection) =
        new_store("coord-complete-one").await;
    let job_id = unique_id("coord-complete-one-job");
    let mut state = fixture_state(&job_id);
    let first = Utc::now();
    let second = first + chrono::TimeDelta::milliseconds(10);
    let config = lease_config();

    let runtime = load_runtime(&store, &job_id, state.clone()).await;
    let first_claim =
        claim_occurrence(&store, &job_id, runtime.revision, &state, first, 1, config).await;
    state.trigger_count = 2;
    let second_claim = claim_occurrence(
        &store,
        &job_id,
        first_claim.state.revision,
        &state,
        second,
        2,
        config,
    )
    .await;

    assert!(
        complete_claim(
            &store,
            &job_id,
            &first_claim,
            CoordinatedCompletion {
                last_run_at: Utc::now(),
                last_success_at: Some(Utc::now()),
                last_error: None,
            },
        )
        .await
    );
    assert_eq!(
        store
            .renew(&second_claim.lease, config)
            .await
            .expect("renew second failed"),
        ExecutionGuardRenewal::Renewed
    );

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
