use super::keys::{FIELD_PAUSED, FIELD_STATE, FIELD_VERSION};
use crate::coordinated_store::CoordinatedRuntimeState;
use crate::model::JobState;
use crate::valkey_store::ValkeyStoreError;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

pub(super) fn parse_runtime_state(
    fields: &HashMap<String, String>,
) -> Result<CoordinatedRuntimeState, ValkeyStoreError> {
    let revision = fields
        .get(FIELD_VERSION)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let paused = fields
        .get(FIELD_PAUSED)
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let state = serde_json::from_str(fields.get(FIELD_STATE).map(String::as_str).unwrap_or("{}"))
        .map_err(ValkeyStoreError::from)?;
    Ok(CoordinatedRuntimeState {
        state,
        revision,
        paused,
    })
}

pub(super) fn parse_job_state(payload: &str) -> Result<JobState, ValkeyStoreError> {
    serde_json::from_str(payload).map_err(ValkeyStoreError::from)
}

pub(super) fn parse_utc_rfc3339(raw: &str) -> Result<DateTime<Utc>, ValkeyStoreError> {
    DateTime::parse_from_rfc3339(raw)
        .map_err(|error| {
            ValkeyStoreError::Codec(serde_json::Error::io(std::io::Error::other(
                error.to_string(),
            )))
        })
        .map(|value| value.with_timezone(&Utc))
}

pub(super) fn serialize_state(state: &JobState) -> Result<String, ValkeyStoreError> {
    serde_json::to_string(state).map_err(ValkeyStoreError::from)
}
