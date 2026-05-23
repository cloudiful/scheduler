use super::ValkeyCoordinatedStateStore;
use super::codec::{parse_job_state, parse_utc_rfc3339};
use super::keys::{
    FIELD_INFLIGHT_CATCH_UP, FIELD_INFLIGHT_LEASE_EXPIRES_AT, FIELD_INFLIGHT_LEASE_KEY,
    FIELD_INFLIGHT_RESOURCE_ID, FIELD_INFLIGHT_SCHEDULED_AT, FIELD_INFLIGHT_SCOPE,
    FIELD_INFLIGHT_SOURCE, FIELD_INFLIGHT_TOKEN, FIELD_INFLIGHT_TRIGGER_COUNT, FIELD_PAUSED,
    FIELD_STATE, FIELD_VERSION, parse_scope, parse_trigger_source, scope_to_str,
    trigger_source_to_str,
};
use crate::coordinated_store::{
    CoordinatedClaim, CoordinatedCompletion, CoordinatedLeaseConfig, CoordinatedPendingTrigger,
    CoordinatedRuntimeState,
};
use crate::execution_guard::{ExecutionGuardRenewal, ExecutionGuardScope, ExecutionLease};
use crate::model::JobState;
use crate::valkey_execution_support::{next_token, now_millis};
use crate::valkey_runtime::ValkeyCommandOutcome;
use crate::valkey_scripts;
use crate::valkey_store::ValkeyStoreError;
use chrono::SecondsFormat;
use std::sync::atomic::AtomicU64;

static COORDINATED_TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);

impl ValkeyCoordinatedStateStore {
    pub(super) async fn reclaim_inflight_claim(
        &self,
        job_id: &str,
        resource_id: &str,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, ValkeyStoreError> {
        let key = self.state_key(job_id);
        let token = next_token(&COORDINATED_TOKEN_COUNTER, "coord");
        let ttl_millis = u64::try_from(lease_config.ttl.as_millis()).unwrap_or(u64::MAX);
        let now_millis = now_millis();
        let expires_at_millis = now_millis.saturating_add(ttl_millis);
        let resource_lock_key = self.resource_lock_key(resource_id);
        let occurrence_index_key = self.occurrence_index_key(resource_id);
        let inflight_index_key = self.inflight_index_key(job_id);
        let result: ValkeyCommandOutcome<Option<Vec<String>>> = self
            .runtime
            .execute({
                let token = token.clone();
                move |mut connection| {
                    let key = key.clone();
                    let resource_lock_key = resource_lock_key.clone();
                    let occurrence_index_key = occurrence_index_key.clone();
                    let inflight_index_key = inflight_index_key.clone();
                    let token = token.clone();
                    async move {
                        valkey_scripts::script(valkey_scripts::coordinated::RECLAIM_INFLIGHT)
                            .key(key)
                            .key(resource_lock_key)
                            .key(occurrence_index_key)
                            .key(inflight_index_key)
                            .arg(FIELD_STATE)
                            .arg(FIELD_VERSION)
                            .arg(FIELD_PAUSED)
                            .arg(now_millis)
                            .arg(&token)
                            .arg(ttl_millis)
                            .arg(expires_at_millis)
                            .arg(FIELD_INFLIGHT_SCHEDULED_AT)
                            .arg(FIELD_INFLIGHT_CATCH_UP)
                            .arg(FIELD_INFLIGHT_TRIGGER_COUNT)
                            .arg(FIELD_INFLIGHT_SOURCE)
                            .arg(FIELD_INFLIGHT_RESOURCE_ID)
                            .arg(FIELD_INFLIGHT_SCOPE)
                            .arg(FIELD_INFLIGHT_LEASE_KEY)
                            .arg(FIELD_INFLIGHT_LEASE_EXPIRES_AT)
                            .arg(FIELD_INFLIGHT_TOKEN)
                            .invoke_async(&mut connection)
                            .await
                    }
                }
            })
            .await
            .map_err(ValkeyStoreError::from)?;

        let ValkeyCommandOutcome::Available(result) = result else {
            return Err(ValkeyStoreError::Unavailable);
        };
        result
            .filter(|values| values.len() == 9)
            .map(|values| claim_from_script_values(job_id, resource_id, values, true))
            .transpose()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn claim_trigger_for_scope(
        &self,
        job_id: &str,
        resource_id: &str,
        revision: u64,
        trigger: CoordinatedPendingTrigger,
        next_state: &JobState,
        lease_config: CoordinatedLeaseConfig,
        scope: ExecutionGuardScope,
    ) -> Result<Option<CoordinatedClaim>, ValkeyStoreError> {
        let key = self.state_key(job_id);
        let lease_key = match scope {
            ExecutionGuardScope::Occurrence => {
                self.occurrence_lease_key(resource_id, trigger.scheduled_at)
            }
            ExecutionGuardScope::Resource => self.resource_lock_key(resource_id),
        };
        let occurrence_lease_key = self.occurrence_lease_key(resource_id, trigger.scheduled_at);
        let token = next_token(&COORDINATED_TOKEN_COUNTER, "coord");
        let ttl_millis = u64::try_from(lease_config.ttl.as_millis()).unwrap_or(u64::MAX);
        let now_millis = now_millis();
        let expires_at_millis = now_millis.saturating_add(ttl_millis);
        let next_state_payload =
            serde_json::to_string(next_state).map_err(ValkeyStoreError::from)?;
        let resource_lock_key = self.resource_lock_key(resource_id);
        let occurrence_index_key = self.occurrence_index_key(resource_id);
        let inflight_index_key = self.inflight_index_key(job_id);
        let scheduled_at = trigger
            .scheduled_at
            .to_rfc3339_opts(SecondsFormat::Nanos, true);
        let resource_id_arg = resource_id.to_string();
        let scope_arg = scope_to_str(scope).to_string();
        let source_arg = trigger_source_to_str(trigger.source).to_string();
        let command_trigger = trigger.clone();
        let result: ValkeyCommandOutcome<i64> = self
            .runtime
            .execute({
                let token = token.clone();
                let command_trigger = command_trigger.clone();
                move |mut connection| {
                    let key = key.clone();
                    let resource_lock_key = resource_lock_key.clone();
                    let occurrence_lease_key = occurrence_lease_key.clone();
                    let occurrence_index_key = occurrence_index_key.clone();
                    let inflight_index_key = inflight_index_key.clone();
                    let token = token.clone();
                    let next_state_payload = next_state_payload.clone();
                    let scheduled_at = scheduled_at.clone();
                    let resource_id_arg = resource_id_arg.clone();
                    let scope_arg = scope_arg.clone();
                    let source_arg = source_arg.clone();
                    let command_trigger = command_trigger.clone();
                    async move {
                        valkey_scripts::script(valkey_scripts::coordinated::CLAIM_TRIGGER)
                            .key(key)
                            .key(resource_lock_key)
                            .key(&occurrence_lease_key)
                            .key(occurrence_index_key)
                            .key(inflight_index_key)
                            .arg(FIELD_VERSION)
                            .arg(FIELD_PAUSED)
                            .arg(now_millis)
                            .arg(revision)
                            .arg(&token)
                            .arg(ttl_millis)
                            .arg(expires_at_millis)
                            .arg(FIELD_STATE)
                            .arg(next_state_payload)
                            .arg(scheduled_at)
                            .arg(command_trigger.catch_up)
                            .arg(command_trigger.trigger_count)
                            .arg(source_arg)
                            .arg(resource_id_arg)
                            .arg(scope_arg)
                            .invoke_async(&mut connection)
                            .await
                    }
                }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        let ValkeyCommandOutcome::Available(new_revision) = result else {
            return Err(ValkeyStoreError::Unavailable);
        };

        if new_revision <= 0 {
            return Ok(None);
        }

        Ok(Some(CoordinatedClaim {
            state: CoordinatedRuntimeState {
                state: next_state.clone(),
                revision: new_revision as u64,
                paused: false,
            },
            trigger: trigger.clone(),
            lease: ExecutionLease::new(
                job_id.to_string(),
                resource_id.to_string(),
                scope,
                match scope {
                    ExecutionGuardScope::Occurrence => Some(trigger.scheduled_at),
                    ExecutionGuardScope::Resource => None,
                },
                token,
                lease_key,
            ),
            replayed: false,
        }))
    }

    pub(super) async fn renew_claim_lease(
        &self,
        lease: &ExecutionLease,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<ExecutionGuardRenewal, ValkeyStoreError> {
        let ttl_millis = u64::try_from(lease_config.ttl.as_millis()).unwrap_or(u64::MAX);
        let expires_at_millis = now_millis().saturating_add(ttl_millis);
        let lease = lease.clone();
        let occurrence_index_key = self.occurrence_index_key(&lease.resource_id);
        let state_key = self.state_key(&lease.job_id);
        let inflight_index_key = self.inflight_index_key(&lease.job_id);
        let scope_arg = scope_to_str(lease.scope).to_string();
        let result: ValkeyCommandOutcome<i32> = self
            .runtime
            .execute(move |mut connection| {
                let lease = lease.clone();
                let occurrence_index_key = occurrence_index_key.clone();
                let state_key = state_key.clone();
                let inflight_index_key = inflight_index_key.clone();
                let scope_arg = scope_arg.clone();
                async move {
                    valkey_scripts::script(valkey_scripts::coordinated::RENEW_LEASE)
                        .key(&lease.lease_key)
                        .key(occurrence_index_key)
                        .key(state_key)
                        .key(inflight_index_key)
                        .arg(&lease.token)
                        .arg(ttl_millis)
                        .arg(expires_at_millis)
                        .arg(scope_arg)
                        .invoke_async(&mut connection)
                        .await
                }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        let ValkeyCommandOutcome::Available(renewed) = result else {
            return Ok(ExecutionGuardRenewal::Lost);
        };
        Ok(if renewed == 1 {
            ExecutionGuardRenewal::Renewed
        } else {
            ExecutionGuardRenewal::Lost
        })
    }

    pub(super) async fn complete_claim(
        &self,
        job_id: &str,
        lease: &ExecutionLease,
        completion: CoordinatedCompletion,
    ) -> Result<bool, ValkeyStoreError> {
        let key = self.state_key(job_id);
        let lease = lease.clone();
        let occurrence_index_key = self.occurrence_index_key(&lease.resource_id);
        let inflight_index_key = self.inflight_index_key(job_id);
        let last_run_at = completion
            .last_run_at
            .to_rfc3339_opts(SecondsFormat::Nanos, true);
        let last_success_at = completion
            .last_success_at
            .map(|value| value.to_rfc3339_opts(SecondsFormat::Nanos, true))
            .unwrap_or_default();
        let last_error = completion.last_error.unwrap_or_default();
        let result: ValkeyCommandOutcome<i32> = self
            .runtime
            .execute(move |mut connection| {
                let key = key.clone();
                let lease = lease.clone();
                let occurrence_index_key = occurrence_index_key.clone();
                let inflight_index_key = inflight_index_key.clone();
                let last_run_at = last_run_at.clone();
                let last_success_at = last_success_at.clone();
                let last_error = last_error.clone();
                async move {
                    valkey_scripts::script(valkey_scripts::coordinated::COMPLETE)
                        .key(key)
                        .key(&lease.lease_key)
                        .key(occurrence_index_key)
                        .key(inflight_index_key)
                        .arg(&lease.token)
                        .arg(FIELD_VERSION)
                        .arg(FIELD_STATE)
                        .arg(last_run_at)
                        .arg(last_success_at)
                        .arg(last_error)
                        .arg(FIELD_INFLIGHT_TOKEN)
                        .arg(FIELD_INFLIGHT_SCOPE)
                        .arg(FIELD_INFLIGHT_SCHEDULED_AT)
                        .arg(FIELD_INFLIGHT_CATCH_UP)
                        .arg(FIELD_INFLIGHT_TRIGGER_COUNT)
                        .arg(FIELD_INFLIGHT_SOURCE)
                        .arg(FIELD_INFLIGHT_RESOURCE_ID)
                        .arg(FIELD_INFLIGHT_LEASE_KEY)
                        .invoke_async(&mut connection)
                        .await
                }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        match result {
            ValkeyCommandOutcome::Available(completed) => Ok(completed == 1),
            ValkeyCommandOutcome::Degraded => Err(ValkeyStoreError::Unavailable),
        }
    }
}

fn claim_from_script_values(
    job_id: &str,
    resource_id: &str,
    values: Vec<String>,
    replayed: bool,
) -> Result<CoordinatedClaim, ValkeyStoreError> {
    let revision = values[0].parse::<u64>().unwrap_or(0);
    let state = parse_job_state(&values[1])?;
    let scheduled_at = parse_utc_rfc3339(&values[2])?;
    let catch_up = values[3].parse::<bool>().unwrap_or(false);
    let trigger_count = values[4].parse::<u32>().unwrap_or(0);
    let source = parse_trigger_source(&values[5]);
    let scope = parse_scope(&values[6]);
    Ok(CoordinatedClaim {
        state: CoordinatedRuntimeState {
            state,
            revision,
            paused: false,
        },
        trigger: CoordinatedPendingTrigger {
            scheduled_at,
            catch_up,
            trigger_count,
            source,
        },
        lease: ExecutionLease::new(
            job_id.to_string(),
            resource_id.to_string(),
            scope,
            Some(scheduled_at),
            values[8].clone(),
            values[7].clone(),
        ),
        replayed,
    })
}
