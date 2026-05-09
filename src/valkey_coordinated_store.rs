use crate::coordinated_store::{
    CoordinatedClaim, CoordinatedLeaseConfig, CoordinatedPendingTrigger, CoordinatedRuntimeState,
    CoordinatedStateStore,
};
use crate::error::{ExecutionGuardErrorKind, StoreErrorKind};
use crate::execution_guard::{ExecutionGuardRenewal, ExecutionGuardScope, ExecutionLease};
use crate::model::JobState;
use crate::valkey_execution_support::{
    next_token, now_millis, occurrence_index_key, occurrence_lease_key, resource_lock_key,
};
use crate::valkey_scripts;
use crate::valkey_store::ValkeyStoreError;
use chrono::SecondsFormat;
use chrono::{DateTime, Utc};
use redis::{AsyncCommands, Client, aio::ConnectionManager, cmd};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

const DEFAULT_STATE_KEY_PREFIX: &str = "scheduler:valkey:job-state:";
const LEGACY_DEFAULT_STATE_KEY_PREFIX: &str = "scheduler:job-state:";
const DEFAULT_EXECUTION_KEY_PREFIX: &str = "scheduler:valkey:execution-lease:";

const FIELD_VERSION: &str = "version";
const FIELD_STATE: &str = "state";
const FIELD_PAUSED: &str = "paused";
const FIELD_INFLIGHT_SCHEDULED_AT: &str = "inflight_scheduled_at";
const FIELD_INFLIGHT_CATCH_UP: &str = "inflight_catch_up";
const FIELD_INFLIGHT_TRIGGER_COUNT: &str = "inflight_trigger_count";
const FIELD_INFLIGHT_RESOURCE_ID: &str = "inflight_resource_id";
const FIELD_INFLIGHT_SCOPE: &str = "inflight_scope";
const FIELD_INFLIGHT_TOKEN: &str = "inflight_token";
const FIELD_INFLIGHT_LEASE_KEY: &str = "inflight_lease_key";
const FIELD_INFLIGHT_LEASE_EXPIRES_AT: &str = "inflight_lease_expires_at";

static COORDINATED_TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct ValkeyCoordinatedStateStore {
    connection: ConnectionManager,
    state_key_prefix: String,
    execution_key_prefix: String,
}

impl ValkeyCoordinatedStateStore {
    pub async fn new(url: impl AsRef<str>) -> Result<Self, redis::RedisError> {
        Self::with_prefixes(url, DEFAULT_STATE_KEY_PREFIX, DEFAULT_EXECUTION_KEY_PREFIX).await
    }

    pub async fn with_prefixes(
        url: impl AsRef<str>,
        state_key_prefix: impl Into<String>,
        execution_key_prefix: impl Into<String>,
    ) -> Result<Self, redis::RedisError> {
        let client = Client::open(url.as_ref())?;
        let connection = client.get_connection_manager().await?;
        Ok(Self {
            connection,
            state_key_prefix: state_key_prefix.into(),
            execution_key_prefix: execution_key_prefix.into(),
        })
    }

    fn state_key(&self, job_id: &str) -> String {
        format!("{}{}", self.state_key_prefix, job_id)
    }

    fn legacy_state_key(&self, job_id: &str) -> Option<String> {
        if self.state_key_prefix == DEFAULT_STATE_KEY_PREFIX {
            Some(format!("{LEGACY_DEFAULT_STATE_KEY_PREFIX}{job_id}"))
        } else {
            None
        }
    }

    fn resource_lock_key(&self, resource_id: &str) -> String {
        resource_lock_key(&self.execution_key_prefix, resource_id)
    }

    fn occurrence_index_key(&self, resource_id: &str) -> String {
        occurrence_index_key(&self.execution_key_prefix, resource_id)
    }

    fn occurrence_lease_key(&self, resource_id: &str, scheduled_at: DateTime<Utc>) -> String {
        occurrence_lease_key(&self.execution_key_prefix, resource_id, scheduled_at)
    }

    async fn key_type(&self, key: &str) -> Result<String, ValkeyStoreError> {
        let mut connection = self.connection.clone();
        cmd("TYPE")
            .arg(key)
            .query_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)
    }

    async fn load_hash(
        &self,
        key: &str,
    ) -> Result<Option<CoordinatedRuntimeState>, ValkeyStoreError> {
        let mut connection = self.connection.clone();
        let fields: HashMap<String, String> = connection
            .hgetall(key)
            .await
            .map_err(ValkeyStoreError::from)?;
        if fields.is_empty() {
            return Ok(None);
        }

        Ok(Some(parse_runtime_state(&fields)?))
    }

    async fn migrate_string_state(
        &self,
        key: &str,
        payload: String,
    ) -> Result<CoordinatedRuntimeState, ValkeyStoreError> {
        let state: JobState = serde_json::from_str(&payload).map_err(ValkeyStoreError::from)?;
        let runtime = CoordinatedRuntimeState {
            state,
            revision: 0,
            paused: false,
        };
        self.write_runtime(key, &runtime).await?;
        Ok(runtime)
    }

    async fn write_runtime(
        &self,
        key: &str,
        runtime: &CoordinatedRuntimeState,
    ) -> Result<(), ValkeyStoreError> {
        let mut connection = self.connection.clone();
        let payload = serde_json::to_string(&runtime.state).map_err(ValkeyStoreError::from)?;
        let _: () = cmd("DEL")
            .arg(key)
            .query_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;
        let _: () = cmd("HSET")
            .arg(key)
            .arg(FIELD_VERSION)
            .arg(runtime.revision)
            .arg(FIELD_STATE)
            .arg(payload)
            .arg(FIELD_PAUSED)
            .arg(if runtime.paused { "1" } else { "0" })
            .query_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;
        Ok(())
    }

    async fn load_payload_state(&self, key: &str) -> Result<Option<String>, ValkeyStoreError> {
        let mut connection = self.connection.clone();
        connection.get(key).await.map_err(ValkeyStoreError::from)
    }
}

impl CoordinatedStateStore for ValkeyCoordinatedStateStore {
    type Error = ValkeyStoreError;

    async fn load_or_initialize(
        &self,
        job_id: &str,
        initial_state: JobState,
    ) -> Result<CoordinatedRuntimeState, Self::Error> {
        let key = self.state_key(job_id);
        match self.key_type(&key).await?.as_str() {
            "hash" => {
                if let Some(runtime) = self.load_hash(&key).await? {
                    return Ok(runtime);
                }
            }
            "string" => {
                if let Some(payload) = self.load_payload_state(&key).await? {
                    return self.migrate_string_state(&key, payload).await;
                }
            }
            "none" => {}
            _ => {}
        }

        if let Some(legacy_key) = self.legacy_state_key(job_id) {
            if self.key_type(&legacy_key).await?.as_str() == "string" {
                if let Some(payload) = self.load_payload_state(&legacy_key).await? {
                    let runtime = self.migrate_string_state(&key, payload).await?;
                    let mut connection = self.connection.clone();
                    let _: () = cmd("DEL")
                        .arg(legacy_key)
                        .query_async(&mut connection)
                        .await
                        .map_err(ValkeyStoreError::from)?;
                    return Ok(runtime);
                }
            }
        }

        let runtime = CoordinatedRuntimeState {
            state: initial_state,
            revision: 0,
            paused: false,
        };
        self.write_runtime(&key, &runtime).await?;
        Ok(runtime)
    }

    async fn save_state(
        &self,
        job_id: &str,
        revision: u64,
        state: &JobState,
    ) -> Result<bool, Self::Error> {
        let key = self.state_key(job_id);
        let payload = serde_json::to_string(state).map_err(ValkeyStoreError::from)?;
        let mut connection = self.connection.clone();
        let updated: i32 = valkey_scripts::script(valkey_scripts::coordinated::SAVE_STATE)
        .key(key)
        .arg(FIELD_VERSION)
        .arg(revision)
            .arg(FIELD_INFLIGHT_TOKEN)
            .arg(FIELD_STATE)
            .arg(payload)
            .invoke_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;
        Ok(updated == 1)
    }

    async fn reclaim_inflight(
        &self,
        job_id: &str,
        resource_id: &str,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        let key = self.state_key(job_id);
        let lease_key = self.occurrence_lease_key(resource_id, Utc::now());
        let token = next_token(&COORDINATED_TOKEN_COUNTER, "coord");
        let ttl_millis = u64::try_from(lease_config.ttl.as_millis()).unwrap_or(u64::MAX);
        let now_millis = now_millis();
        let expires_at_millis = now_millis.saturating_add(ttl_millis);
        let mut connection = self.connection.clone();
        let result: Option<Vec<String>> =
            valkey_scripts::script(valkey_scripts::coordinated::RECLAIM_INFLIGHT)
        .key(key)
        .key(self.resource_lock_key(resource_id))
        .key(lease_key.clone())
            .key(self.occurrence_index_key(resource_id))
            // ARGV layout:
            //  1..9  = hash field names read from KEYS[1]
            // 10..14 = reclaim timing + new lease payload
            // 15..16 = hash field names updated on successful reclaim
            .arg(FIELD_INFLIGHT_SCHEDULED_AT)
            .arg(FIELD_INFLIGHT_CATCH_UP)
            .arg(FIELD_INFLIGHT_TRIGGER_COUNT)
            .arg(FIELD_INFLIGHT_RESOURCE_ID)
            .arg(FIELD_INFLIGHT_SCOPE)
            .arg(FIELD_INFLIGHT_LEASE_EXPIRES_AT)
            .arg(FIELD_STATE)
            .arg(FIELD_VERSION)
            .arg(FIELD_PAUSED)
            .arg(now_millis)
            .arg(format!(
                "{}{}:occurrence:",
                self.execution_key_prefix, resource_id
            ))
            .arg(&token)
            .arg(ttl_millis)
            .arg(expires_at_millis)
            .arg(FIELD_INFLIGHT_TOKEN)
            .arg(FIELD_INFLIGHT_LEASE_KEY)
            .invoke_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;

        let Some(values) = result else {
            return Ok(None);
        };
        if values.len() != 8 {
            return Ok(None);
        }
        let revision = values[0].parse::<u64>().unwrap_or(0);
        let state: JobState = serde_json::from_str(&values[1]).map_err(ValkeyStoreError::from)?;
        let scheduled_at = DateTime::parse_from_rfc3339(&values[2])
            .map_err(|error| {
                ValkeyStoreError::Codec(serde_json::Error::io(std::io::Error::other(
                    error.to_string(),
                )))
            })?
            .with_timezone(&Utc);
        let catch_up = values[3].parse::<bool>().unwrap_or(false);
        let trigger_count = values[4].parse::<u32>().unwrap_or(0);
        let scope = parse_scope(&values[5]);
        Ok(Some(CoordinatedClaim {
            state: CoordinatedRuntimeState {
                state,
                revision,
                paused: false,
            },
            trigger: CoordinatedPendingTrigger {
                scheduled_at,
                catch_up,
                trigger_count,
            },
            lease: ExecutionLease::new(
                job_id.to_string(),
                resource_id.to_string(),
                scope,
                Some(scheduled_at),
                values[7].clone(),
                values[6].clone(),
            ),
            replayed: true,
        }))
    }

    async fn claim_trigger(
        &self,
        job_id: &str,
        resource_id: &str,
        revision: u64,
        trigger: CoordinatedPendingTrigger,
        next_state: &JobState,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        let key = self.state_key(job_id);
        let lease_key = self.occurrence_lease_key(resource_id, trigger.scheduled_at);
        let token = next_token(&COORDINATED_TOKEN_COUNTER, "coord");
        let ttl_millis = u64::try_from(lease_config.ttl.as_millis()).unwrap_or(u64::MAX);
        let now_millis = now_millis();
        let expires_at_millis = now_millis.saturating_add(ttl_millis);
        let next_state_payload =
            serde_json::to_string(next_state).map_err(ValkeyStoreError::from)?;
        let mut connection = self.connection.clone();
        let new_revision: i64 = valkey_scripts::script(valkey_scripts::coordinated::CLAIM_TRIGGER)
        .key(key)
        .key(self.resource_lock_key(resource_id))
        .key(&lease_key)
            .key(self.occurrence_index_key(resource_id))
            .arg(FIELD_VERSION)
            .arg(FIELD_INFLIGHT_TOKEN)
            .arg(FIELD_INFLIGHT_LEASE_EXPIRES_AT)
            .arg(FIELD_PAUSED)
            .arg(now_millis)
            .arg(revision)
            .arg(&token)
            .arg(ttl_millis)
            .arg(expires_at_millis)
            .arg(FIELD_STATE)
            .arg(next_state_payload)
            .arg(FIELD_INFLIGHT_SCHEDULED_AT)
            .arg(
                trigger
                    .scheduled_at
                    .to_rfc3339_opts(SecondsFormat::Nanos, true),
            )
            .arg(FIELD_INFLIGHT_CATCH_UP)
            .arg(trigger.catch_up)
            .arg(FIELD_INFLIGHT_TRIGGER_COUNT)
            .arg(trigger.trigger_count)
            .arg(FIELD_INFLIGHT_RESOURCE_ID)
            .arg(resource_id)
            .arg(FIELD_INFLIGHT_SCOPE)
            .arg("occurrence")
            .arg(FIELD_INFLIGHT_TOKEN)
            .arg(FIELD_INFLIGHT_LEASE_KEY)
            .invoke_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;

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
                ExecutionGuardScope::Occurrence,
                Some(trigger.scheduled_at),
                token,
                lease_key,
            ),
            replayed: false,
        }))
    }

    async fn renew(
        &self,
        lease: &ExecutionLease,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<ExecutionGuardRenewal, Self::Error> {
        let ttl_millis = u64::try_from(lease_config.ttl.as_millis()).unwrap_or(u64::MAX);
        let expires_at_millis = now_millis().saturating_add(ttl_millis);
        let mut connection = self.connection.clone();
        let renewed: i32 = valkey_scripts::script(valkey_scripts::coordinated::RENEW_LEASE)
        .key(&lease.lease_key)
        .key(self.occurrence_index_key(&lease.resource_id))
        .key(self.state_key(&lease.job_id))
            .arg(&lease.token)
            .arg(ttl_millis)
            .arg(expires_at_millis)
            .arg(FIELD_INFLIGHT_LEASE_EXPIRES_AT)
            .invoke_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;
        Ok(if renewed == 1 {
            ExecutionGuardRenewal::Renewed
        } else {
            ExecutionGuardRenewal::Lost
        })
    }

    async fn complete(
        &self,
        job_id: &str,
        revision: u64,
        lease: &ExecutionLease,
        state: &JobState,
    ) -> Result<bool, Self::Error> {
        let key = self.state_key(job_id);
        let payload = serde_json::to_string(state).map_err(ValkeyStoreError::from)?;
        let mut connection = self.connection.clone();
        let completed: i32 = valkey_scripts::script(valkey_scripts::coordinated::COMPLETE)
        .key(key)
        .key(&lease.lease_key)
        .key(self.occurrence_index_key(&lease.resource_id))
            .arg(FIELD_VERSION)
            .arg(FIELD_INFLIGHT_TOKEN)
            .arg(revision)
            .arg(&lease.token)
            .arg(FIELD_STATE)
            .arg(payload)
            .arg(FIELD_INFLIGHT_SCHEDULED_AT)
            .arg(FIELD_INFLIGHT_CATCH_UP)
            .arg(FIELD_INFLIGHT_TRIGGER_COUNT)
            .arg(FIELD_INFLIGHT_RESOURCE_ID)
            .arg(FIELD_INFLIGHT_SCOPE)
            .arg(FIELD_INFLIGHT_LEASE_KEY)
            .invoke_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;
        Ok(completed == 1)
    }

    async fn delete(&self, job_id: &str) -> Result<(), Self::Error> {
        let key = self.state_key(job_id);
        let mut connection = self.connection.clone();
        let _: () = cmd("DEL")
            .arg(key)
            .query_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;
        Ok(())
    }

    async fn pause(&self, job_id: &str) -> Result<bool, Self::Error> {
        let key = self.state_key(job_id);
        let mut connection = self.connection.clone();
        let changed: i32 = valkey_scripts::script(valkey_scripts::coordinated::PAUSE)
        .key(key)
        .arg(FIELD_PAUSED)
        .invoke_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;
        Ok(changed == 1)
    }

    async fn resume(&self, job_id: &str) -> Result<bool, Self::Error> {
        let key = self.state_key(job_id);
        let mut connection = self.connection.clone();
        let changed: i32 = valkey_scripts::script(valkey_scripts::coordinated::RESUME)
        .key(key)
        .arg(FIELD_PAUSED)
        .invoke_async(&mut connection)
            .await
            .map_err(ValkeyStoreError::from)?;
        Ok(changed == 1)
    }

    fn classify_store_error(error: &Self::Error) -> StoreErrorKind
    where
        Self: Sized,
    {
        if matches!(error, ValkeyStoreError::Codec(_)) {
            StoreErrorKind::Data
        } else if error.is_connection_issue() {
            StoreErrorKind::Connection
        } else {
            StoreErrorKind::Unknown
        }
    }

    fn classify_guard_error(error: &Self::Error) -> ExecutionGuardErrorKind
    where
        Self: Sized,
    {
        if matches!(error, ValkeyStoreError::Codec(_)) {
            ExecutionGuardErrorKind::Data
        } else if error.is_connection_issue() {
            ExecutionGuardErrorKind::Connection
        } else {
            ExecutionGuardErrorKind::Unknown
        }
    }
}

fn parse_runtime_state(
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

fn parse_scope(raw: &str) -> ExecutionGuardScope {
    match raw {
        "resource" => ExecutionGuardScope::Resource,
        _ => ExecutionGuardScope::Occurrence,
    }
}
