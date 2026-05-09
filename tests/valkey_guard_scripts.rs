#![cfg(feature = "valkey-guard")]

#[path = "support/valkey_cleanup.rs"]
mod valkey_cleanup;
#[path = "support/valkey_runtime.rs"]
mod valkey_runtime;

use chrono::Utc;
use redis::AsyncCommands;
use scheduler::{
    ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardRenewal, ExecutionSlot,
    ValkeyExecutionGuard, ValkeyLeaseConfig,
};
use std::time::Duration;
use valkey_cleanup::delete_matching_prefix;
use valkey_runtime::{connection, unique_id, valkey_url};

fn lease_config() -> ValkeyLeaseConfig {
    ValkeyLeaseConfig {
        ttl: Duration::from_millis(80),
        renew_interval: Duration::from_millis(20),
    }
}

async fn new_guard(
    test_name: &str,
) -> (
    String,
    ValkeyExecutionGuard,
    redis::aio::MultiplexedConnection,
) {
    let url = valkey_url().expect("SCHEDULER_VALKEY_URL must be set");
    let prefix = format!("scheduler:test:{test_name}:{}:", unique_id(test_name));
    let guard = ValkeyExecutionGuard::with_prefix(&url, prefix.clone(), lease_config())
        .await
        .expect("failed to create guard");
    let connection = connection(&url).await;
    (prefix, guard, connection)
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn acquire_occurrence_script_acquires_occurrence_lock() {
    let (prefix, guard, mut connection) = new_guard("guard-acquire-occurrence").await;
    let slot = ExecutionSlot::for_occurrence("job", "resource", Utc::now());

    let acquired = guard.acquire(slot).await.expect("acquire failed");

    assert!(matches!(acquired, ExecutionGuardAcquire::Acquired(_)));

    delete_matching_prefix(&mut connection, &prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn acquire_resource_script_blocks_when_occurrence_exists() {
    let (prefix, guard, mut connection) = new_guard("guard-acquire-resource").await;
    let occurrence = ExecutionSlot::for_occurrence("job", "resource", Utc::now());
    let resource = ExecutionSlot::for_resource("job", "resource");

    let acquired = guard
        .acquire(occurrence)
        .await
        .expect("occurrence acquire failed");
    assert!(matches!(acquired, ExecutionGuardAcquire::Acquired(_)));

    let contended = guard
        .acquire(resource)
        .await
        .expect("resource acquire failed");

    assert!(matches!(contended, ExecutionGuardAcquire::Contended));

    delete_matching_prefix(&mut connection, &prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn renew_occurrence_script_extends_owned_occurrence_lease() {
    let (prefix, guard, mut connection) = new_guard("guard-renew-occurrence").await;
    let slot = ExecutionSlot::for_occurrence("job", "resource", Utc::now());
    let lease = match guard.acquire(slot).await.expect("acquire failed") {
        ExecutionGuardAcquire::Acquired(lease) => lease,
        ExecutionGuardAcquire::Contended => panic!("expected acquired lease"),
    };

    let renewed = guard.renew(&lease).await.expect("renew failed");

    assert_eq!(renewed, ExecutionGuardRenewal::Renewed);

    delete_matching_prefix(&mut connection, &prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn renew_resource_script_extends_owned_resource_lease() {
    let (prefix, guard, mut connection) = new_guard("guard-renew-resource").await;
    let slot = ExecutionSlot::for_resource("job", "resource");
    let lease = match guard.acquire(slot).await.expect("acquire failed") {
        ExecutionGuardAcquire::Acquired(lease) => lease,
        ExecutionGuardAcquire::Contended => panic!("expected acquired lease"),
    };

    let renewed = guard.renew(&lease).await.expect("renew failed");

    assert_eq!(renewed, ExecutionGuardRenewal::Renewed);

    delete_matching_prefix(&mut connection, &prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn release_occurrence_script_deletes_owned_occurrence_lease() {
    let (prefix, guard, mut connection) = new_guard("guard-release-occurrence").await;
    let slot = ExecutionSlot::for_occurrence("job", "resource", Utc::now());
    let lease = match guard.acquire(slot).await.expect("acquire failed") {
        ExecutionGuardAcquire::Acquired(lease) => lease,
        ExecutionGuardAcquire::Contended => panic!("expected acquired lease"),
    };

    guard.release(&lease).await.expect("release failed");

    let remaining: Option<String> = connection
        .get(&lease.lease_key)
        .await
        .expect("failed to read lease key");
    assert!(remaining.is_none());

    delete_matching_prefix(&mut connection, &prefix).await;
}

#[tokio::test]
#[ignore = "requires SCHEDULER_VALKEY_URL pointing to a reachable Valkey server"]
async fn release_resource_script_deletes_owned_resource_lease() {
    let (prefix, guard, mut connection) = new_guard("guard-release-resource").await;
    let slot = ExecutionSlot::for_resource("job", "resource");
    let lease = match guard.acquire(slot).await.expect("acquire failed") {
        ExecutionGuardAcquire::Acquired(lease) => lease,
        ExecutionGuardAcquire::Contended => panic!("expected acquired lease"),
    };

    guard.release(&lease).await.expect("release failed");

    let remaining: Option<String> = connection
        .get(&lease.lease_key)
        .await
        .expect("failed to read lease key");
    assert!(remaining.is_none());

    delete_matching_prefix(&mut connection, &prefix).await;
}
