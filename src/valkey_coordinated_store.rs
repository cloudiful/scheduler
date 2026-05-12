mod codec;
mod inflight_commands;
mod keys;
mod state_commands;

use crate::coordinated_store::{
    CoordinatedClaim, CoordinatedCompletion, CoordinatedLeaseConfig, CoordinatedPendingTrigger,
    CoordinatedRuntimeState, CoordinatedStateStore,
};
use crate::error::{ExecutionGuardErrorKind, StoreErrorKind};
use crate::execution_guard::{ExecutionGuardRenewal, ExecutionGuardScope, ExecutionLease};
use crate::model::JobState;
use crate::valkey_runtime::{ValkeyRecoveryConfig, ValkeyRuntime};
use crate::valkey_store::ValkeyStoreError;
use keys::{DEFAULT_EXECUTION_KEY_PREFIX, DEFAULT_STATE_KEY_PREFIX};
use redis::Client;

#[derive(Debug, Clone)]
pub struct ValkeyCoordinatedStateStore {
    pub(super) runtime: ValkeyRuntime,
    pub(super) state_key_prefix: String,
    pub(super) execution_key_prefix: String,
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
        Self::with_prefixes_and_recovery_config(
            url,
            state_key_prefix,
            execution_key_prefix,
            ValkeyRecoveryConfig::default(),
        )
        .await
    }

    pub async fn with_recovery_config(
        url: impl AsRef<str>,
        config: ValkeyRecoveryConfig,
    ) -> Result<Self, redis::RedisError> {
        Self::with_prefixes_and_recovery_config(
            url,
            DEFAULT_STATE_KEY_PREFIX,
            DEFAULT_EXECUTION_KEY_PREFIX,
            config,
        )
        .await
    }

    pub async fn with_prefixes_and_recovery_config(
        url: impl AsRef<str>,
        state_key_prefix: impl Into<String>,
        execution_key_prefix: impl Into<String>,
        config: ValkeyRecoveryConfig,
    ) -> Result<Self, redis::RedisError> {
        let client = Client::open(url.as_ref())?;
        let runtime = ValkeyRuntime::from_client(client, config).await?;
        Ok(Self {
            runtime,
            state_key_prefix: state_key_prefix.into(),
            execution_key_prefix: execution_key_prefix.into(),
        })
    }
}

impl CoordinatedStateStore for ValkeyCoordinatedStateStore {
    type Error = ValkeyStoreError;

    async fn load_or_initialize(
        &self,
        job_id: &str,
        initial_state: JobState,
    ) -> Result<CoordinatedRuntimeState, Self::Error> {
        self.load_or_initialize_state(job_id, initial_state).await
    }

    async fn save_state(
        &self,
        job_id: &str,
        revision: u64,
        state: &JobState,
    ) -> Result<bool, Self::Error> {
        self.save_runtime_state(job_id, revision, state).await
    }

    async fn reclaim_inflight(
        &self,
        job_id: &str,
        resource_id: &str,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        self.reclaim_inflight_claim(job_id, resource_id, lease_config)
            .await
    }

    async fn claim_trigger(
        &self,
        job_id: &str,
        resource_id: &str,
        revision: u64,
        trigger: CoordinatedPendingTrigger,
        next_state: &JobState,
        lease_config: CoordinatedLeaseConfig,
        scope: ExecutionGuardScope,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        self.claim_trigger_for_scope(
            job_id,
            resource_id,
            revision,
            trigger,
            next_state,
            lease_config,
            scope,
        )
        .await
    }

    async fn renew(
        &self,
        lease: &ExecutionLease,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<ExecutionGuardRenewal, Self::Error> {
        self.renew_claim_lease(lease, lease_config).await
    }

    async fn complete(
        &self,
        job_id: &str,
        lease: &ExecutionLease,
        completion: CoordinatedCompletion,
    ) -> Result<bool, Self::Error> {
        self.complete_claim(job_id, lease, completion).await
    }

    async fn delete(&self, job_id: &str) -> Result<(), Self::Error> {
        self.delete_state(job_id).await
    }

    async fn pause(&self, job_id: &str) -> Result<bool, Self::Error> {
        self.pause_state(job_id).await
    }

    async fn resume(&self, job_id: &str) -> Result<bool, Self::Error> {
        self.resume_state(job_id).await
    }

    fn classify_store_error(error: &Self::Error) -> StoreErrorKind
    where
        Self: Sized,
    {
        classify_store_error(error)
    }

    fn classify_guard_error(error: &Self::Error) -> ExecutionGuardErrorKind
    where
        Self: Sized,
    {
        classify_guard_error(error)
    }
}

fn classify_store_error(error: &ValkeyStoreError) -> StoreErrorKind {
    if matches!(error, ValkeyStoreError::Codec(_)) {
        StoreErrorKind::Data
    } else if error.is_connection_issue() {
        StoreErrorKind::Connection
    } else {
        StoreErrorKind::Unknown
    }
}

fn classify_guard_error(error: &ValkeyStoreError) -> ExecutionGuardErrorKind {
    if matches!(error, ValkeyStoreError::Codec(_)) {
        ExecutionGuardErrorKind::Data
    } else if error.is_connection_issue() {
        ExecutionGuardErrorKind::Connection
    } else {
        ExecutionGuardErrorKind::Unknown
    }
}
