use crate::{ExecutionGuardScope, ExecutionSlot};
use chrono::SecondsFormat;
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn lease_key(prefix: &str, slot: &ExecutionSlot) -> String {
    match slot.scope {
        ExecutionGuardScope::Occurrence => occurrence_lease_key(
            prefix,
            &slot.resource_id,
            slot.scheduled_at
                .expect("occurrence execution slot must include scheduled_at"),
        ),
        ExecutionGuardScope::Resource => resource_lock_key(prefix, &slot.resource_id),
    }
}

pub(crate) fn resource_lock_key(prefix: &str, resource_id: &str) -> String {
    format!("{prefix}{resource_id}:resource")
}

pub(crate) fn occurrence_lease_key(
    prefix: &str,
    resource_id: &str,
    scheduled_at: DateTime<Utc>,
) -> String {
    format!(
        "{}{}:occurrence:{}",
        prefix,
        resource_id,
        scheduled_at.to_rfc3339_opts(SecondsFormat::Nanos, true)
    )
}

pub(crate) fn occurrence_index_key(prefix: &str, resource_id: &str) -> String {
    format!("{prefix}{resource_id}:occurrences")
}

pub(crate) fn next_token(counter: &AtomicU64, prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let sequence = counter.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{now}-{sequence}")
}

pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
