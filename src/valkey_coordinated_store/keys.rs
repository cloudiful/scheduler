use super::ValkeyCoordinatedStateStore;
use crate::execution_guard::ExecutionGuardScope;
use crate::model::TriggerSource;
use crate::valkey_execution_support::{
    occurrence_index_key, occurrence_lease_key, resource_lock_key,
};
use chrono::{DateTime, Utc};

pub(super) const DEFAULT_STATE_KEY_PREFIX: &str = "scheduler:valkey:job-state:";
pub(super) const LEGACY_DEFAULT_STATE_KEY_PREFIX: &str = "scheduler:job-state:";
pub(super) const DEFAULT_EXECUTION_KEY_PREFIX: &str = "scheduler:valkey:execution-lease:";

pub(super) const FIELD_VERSION: &str = "version";
pub(super) const FIELD_STATE: &str = "state";
pub(super) const FIELD_PAUSED: &str = "paused";
pub(super) const FIELD_INFLIGHT_SCHEDULED_AT: &str = "inflight_scheduled_at";
pub(super) const FIELD_INFLIGHT_CATCH_UP: &str = "inflight_catch_up";
pub(super) const FIELD_INFLIGHT_TRIGGER_COUNT: &str = "inflight_trigger_count";
pub(super) const FIELD_INFLIGHT_SOURCE: &str = "inflight_source";
pub(super) const FIELD_INFLIGHT_RESOURCE_ID: &str = "inflight_resource_id";
pub(super) const FIELD_INFLIGHT_SCOPE: &str = "inflight_scope";
pub(super) const FIELD_INFLIGHT_TOKEN: &str = "inflight_token";
pub(super) const FIELD_INFLIGHT_LEASE_KEY: &str = "inflight_lease_key";
pub(super) const FIELD_INFLIGHT_LEASE_EXPIRES_AT: &str = "inflight_lease_expires_at";

impl ValkeyCoordinatedStateStore {
    pub(super) fn state_key(&self, job_id: &str) -> String {
        format!("{}{}", self.state_key_prefix, job_id)
    }

    pub(super) fn legacy_state_key(&self, job_id: &str) -> Option<String> {
        if self.state_key_prefix == DEFAULT_STATE_KEY_PREFIX {
            Some(format!("{LEGACY_DEFAULT_STATE_KEY_PREFIX}{job_id}"))
        } else {
            None
        }
    }

    pub(super) fn resource_lock_key(&self, resource_id: &str) -> String {
        resource_lock_key(&self.execution_key_prefix, resource_id)
    }

    pub(super) fn occurrence_index_key(&self, resource_id: &str) -> String {
        occurrence_index_key(&self.execution_key_prefix, resource_id)
    }

    pub(super) fn occurrence_lease_key(
        &self,
        resource_id: &str,
        scheduled_at: DateTime<Utc>,
    ) -> String {
        occurrence_lease_key(&self.execution_key_prefix, resource_id, scheduled_at)
    }

    pub(super) fn inflight_index_key(&self, job_id: &str) -> String {
        format!("{}{}:inflight", self.state_key_prefix, job_id)
    }
}

pub(super) fn parse_scope(raw: &str) -> ExecutionGuardScope {
    match raw {
        "resource" => ExecutionGuardScope::Resource,
        _ => ExecutionGuardScope::Occurrence,
    }
}

pub(super) fn scope_to_str(scope: ExecutionGuardScope) -> &'static str {
    match scope {
        ExecutionGuardScope::Occurrence => "occurrence",
        ExecutionGuardScope::Resource => "resource",
    }
}

pub(super) fn parse_trigger_source(raw: &str) -> TriggerSource {
    match raw {
        "manual" => TriggerSource::Manual,
        _ => TriggerSource::Scheduled,
    }
}

pub(super) fn trigger_source_to_str(source: TriggerSource) -> &'static str {
    match source {
        TriggerSource::Scheduled => "scheduled",
        TriggerSource::Manual => "manual",
    }
}
