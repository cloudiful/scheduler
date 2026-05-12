use super::ValkeyCoordinatedStateStore;
use super::codec::{parse_job_state, parse_runtime_state, serialize_state};
use super::keys::{FIELD_INFLIGHT_TOKEN, FIELD_PAUSED, FIELD_STATE, FIELD_VERSION};
use crate::coordinated_store::CoordinatedRuntimeState;
use crate::model::JobState;
use crate::valkey_execution_support::now_millis;
use crate::valkey_runtime::ValkeyCommandOutcome;
use crate::valkey_scripts;
use crate::valkey_store::ValkeyStoreError;
use redis::{AsyncCommands, cmd};
use std::collections::HashMap;

impl ValkeyCoordinatedStateStore {
    pub(super) async fn load_or_initialize_state(
        &self,
        job_id: &str,
        initial_state: JobState,
    ) -> Result<CoordinatedRuntimeState, ValkeyStoreError> {
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
                    let result: ValkeyCommandOutcome<()> = self
                        .runtime
                        .execute(move |mut connection| {
                            let legacy_key = legacy_key.clone();
                            async move {
                                cmd("DEL")
                                    .arg(legacy_key)
                                    .query_async(&mut connection)
                                    .await
                            }
                        })
                        .await
                        .map_err(ValkeyStoreError::from)?;
                    if matches!(result, ValkeyCommandOutcome::Degraded) {
                        return Err(ValkeyStoreError::Unavailable);
                    }
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

    pub(super) async fn save_runtime_state(
        &self,
        job_id: &str,
        revision: u64,
        state: &JobState,
    ) -> Result<bool, ValkeyStoreError> {
        let key = self.state_key(job_id);
        let inflight_index_key = self.inflight_index_key(job_id);
        let payload = serialize_state(state)?;
        let now_millis = now_millis();
        let result: ValkeyCommandOutcome<i32> = self
            .runtime
            .execute(move |mut connection| {
                let key = key.clone();
                let inflight_index_key = inflight_index_key.clone();
                let payload = payload.clone();
                async move {
                    valkey_scripts::script(valkey_scripts::coordinated::SAVE_STATE)
                        .key(key)
                        .key(inflight_index_key)
                        .arg(FIELD_VERSION)
                        .arg(revision)
                        .arg(FIELD_INFLIGHT_TOKEN)
                        .arg(FIELD_STATE)
                        .arg(payload)
                        .arg(now_millis)
                        .invoke_async(&mut connection)
                        .await
                }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        match result {
            ValkeyCommandOutcome::Available(updated) => Ok(updated == 1),
            ValkeyCommandOutcome::Degraded => Err(ValkeyStoreError::Unavailable),
        }
    }

    pub(super) async fn delete_state(&self, job_id: &str) -> Result<(), ValkeyStoreError> {
        let key = self.state_key(job_id);
        let result = self
            .runtime
            .execute(move |mut connection| {
                let key = key.clone();
                async move { cmd("DEL").arg(key).query_async(&mut connection).await }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        match result {
            ValkeyCommandOutcome::Available(()) => Ok(()),
            ValkeyCommandOutcome::Degraded => Err(ValkeyStoreError::Unavailable),
        }
    }

    pub(super) async fn pause_state(&self, job_id: &str) -> Result<bool, ValkeyStoreError> {
        self.set_pause(job_id, valkey_scripts::coordinated::PAUSE)
            .await
    }

    pub(super) async fn resume_state(&self, job_id: &str) -> Result<bool, ValkeyStoreError> {
        self.set_pause(job_id, valkey_scripts::coordinated::RESUME)
            .await
    }

    async fn set_pause(
        &self,
        job_id: &str,
        script: &'static str,
    ) -> Result<bool, ValkeyStoreError> {
        let key = self.state_key(job_id);
        let result: ValkeyCommandOutcome<i32> = self
            .runtime
            .execute(move |mut connection| {
                let key = key.clone();
                async move {
                    valkey_scripts::script(script)
                        .key(key)
                        .arg(FIELD_PAUSED)
                        .invoke_async(&mut connection)
                        .await
                }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        match result {
            ValkeyCommandOutcome::Available(changed) => Ok(changed == 1),
            ValkeyCommandOutcome::Degraded => Err(ValkeyStoreError::Unavailable),
        }
    }

    async fn key_type(&self, key: &str) -> Result<String, ValkeyStoreError> {
        let key = key.to_string();
        let result: ValkeyCommandOutcome<String> = self
            .runtime
            .execute(move |mut connection| {
                let key = key.clone();
                async move { cmd("TYPE").arg(key).query_async(&mut connection).await }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        match result {
            ValkeyCommandOutcome::Available(value) => Ok(value),
            ValkeyCommandOutcome::Degraded => Err(ValkeyStoreError::Unavailable),
        }
    }

    async fn load_hash(
        &self,
        key: &str,
    ) -> Result<Option<CoordinatedRuntimeState>, ValkeyStoreError> {
        let key = key.to_string();
        let result: ValkeyCommandOutcome<HashMap<String, String>> = self
            .runtime
            .execute(move |mut connection| {
                let key = key.clone();
                async move { connection.hgetall(key).await }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        let ValkeyCommandOutcome::Available(fields) = result else {
            return Err(ValkeyStoreError::Unavailable);
        };
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
        let state = parse_job_state(&payload)?;
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
        let payload = serialize_state(&runtime.state)?;
        let key = key.to_string();
        let revision = runtime.revision;
        let paused = runtime.paused;
        let result = self
            .runtime
            .execute(move |mut connection| {
                let key = key.clone();
                let payload = payload.clone();
                async move {
                    let _: () = cmd("DEL").arg(&key).query_async(&mut connection).await?;
                    let _: () = cmd("HSET")
                        .arg(key)
                        .arg(FIELD_VERSION)
                        .arg(revision)
                        .arg(FIELD_STATE)
                        .arg(payload)
                        .arg(FIELD_PAUSED)
                        .arg(if paused { "1" } else { "0" })
                        .query_async(&mut connection)
                        .await?;
                    Ok(())
                }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        match result {
            ValkeyCommandOutcome::Available(()) => Ok(()),
            ValkeyCommandOutcome::Degraded => Err(ValkeyStoreError::Unavailable),
        }
    }

    async fn load_payload_state(&self, key: &str) -> Result<Option<String>, ValkeyStoreError> {
        let key = key.to_string();
        let result = self
            .runtime
            .execute(move |mut connection| {
                let key = key.clone();
                async move { connection.get(key).await }
            })
            .await
            .map_err(ValkeyStoreError::from)?;
        match result {
            ValkeyCommandOutcome::Available(value) => Ok(value),
            ValkeyCommandOutcome::Degraded => Err(ValkeyStoreError::Unavailable),
        }
    }
}
