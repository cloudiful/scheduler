use crate::error::{ExecutionGuardErrorKind, StoreErrorKind};
use crate::execution_guard::{ExecutionGuardRenewal, ExecutionGuardScope, ExecutionLease};
use crate::model::JobState;
use chrono::{DateTime, Utc};
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatedLeaseConfig {
    pub ttl: Duration,
    pub renew_interval: Duration,
}

impl CoordinatedLeaseConfig {
    pub fn validate(self) -> Result<Self, &'static str> {
        if self.ttl.is_zero() {
            return Err("coordinated lease ttl must be greater than zero");
        }
        if self.renew_interval.is_zero() {
            return Err("coordinated lease renew_interval must be greater than zero");
        }
        if self.renew_interval >= self.ttl {
            return Err("coordinated lease renew_interval must be less than ttl");
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedRuntimeState {
    pub state: JobState,
    pub revision: u64,
    pub paused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedPendingTrigger {
    pub scheduled_at: DateTime<Utc>,
    pub catch_up: bool,
    pub trigger_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedClaim {
    pub state: CoordinatedRuntimeState,
    pub trigger: CoordinatedPendingTrigger,
    pub lease: ExecutionLease,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedCompletion {
    pub last_run_at: DateTime<Utc>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

pub trait CoordinatedStateStore {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load_or_initialize(
        &self,
        job_id: &str,
        initial_state: JobState,
    ) -> impl Future<Output = Result<CoordinatedRuntimeState, Self::Error>> + Send;

    fn save_state(
        &self,
        job_id: &str,
        revision: u64,
        state: &JobState,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn reclaim_inflight(
        &self,
        job_id: &str,
        resource_id: &str,
        lease_config: CoordinatedLeaseConfig,
    ) -> impl Future<Output = Result<Option<CoordinatedClaim>, Self::Error>> + Send;

    fn claim_trigger(
        &self,
        job_id: &str,
        resource_id: &str,
        revision: u64,
        trigger: CoordinatedPendingTrigger,
        next_state: &JobState,
        lease_config: CoordinatedLeaseConfig,
        scope: ExecutionGuardScope,
    ) -> impl Future<Output = Result<Option<CoordinatedClaim>, Self::Error>> + Send;

    fn renew(
        &self,
        lease: &ExecutionLease,
        lease_config: CoordinatedLeaseConfig,
    ) -> impl Future<Output = Result<ExecutionGuardRenewal, Self::Error>> + Send;

    fn complete(
        &self,
        job_id: &str,
        lease: &ExecutionLease,
        completion: CoordinatedCompletion,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn delete(&self, job_id: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn pause(&self, job_id: &str) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn resume(&self, job_id: &str) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn classify_store_error(_error: &Self::Error) -> StoreErrorKind
    where
        Self: Sized,
    {
        StoreErrorKind::Unknown
    }

    fn classify_guard_error(_error: &Self::Error) -> ExecutionGuardErrorKind
    where
        Self: Sized,
    {
        ExecutionGuardErrorKind::Unknown
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopCoordinatedStateStore;

impl CoordinatedStateStore for NoopCoordinatedStateStore {
    type Error = Infallible;

    async fn load_or_initialize(
        &self,
        _job_id: &str,
        initial_state: JobState,
    ) -> Result<CoordinatedRuntimeState, Self::Error> {
        Ok(CoordinatedRuntimeState {
            state: initial_state,
            revision: 0,
            paused: false,
        })
    }

    async fn save_state(
        &self,
        _job_id: &str,
        _revision: u64,
        _state: &JobState,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }

    async fn reclaim_inflight(
        &self,
        _job_id: &str,
        _resource_id: &str,
        _lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        Ok(None)
    }

    async fn claim_trigger(
        &self,
        _job_id: &str,
        _resource_id: &str,
        _revision: u64,
        _trigger: CoordinatedPendingTrigger,
        _next_state: &JobState,
        _lease_config: CoordinatedLeaseConfig,
        _scope: ExecutionGuardScope,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        Ok(None)
    }

    async fn renew(
        &self,
        _lease: &ExecutionLease,
        _lease_config: CoordinatedLeaseConfig,
    ) -> Result<ExecutionGuardRenewal, Self::Error> {
        Ok(ExecutionGuardRenewal::Lost)
    }

    async fn complete(
        &self,
        _job_id: &str,
        _lease: &ExecutionLease,
        _completion: CoordinatedCompletion,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }

    async fn delete(&self, _job_id: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn pause(&self, _job_id: &str) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn resume(&self, _job_id: &str) -> Result<bool, Self::Error> {
        Ok(false)
    }
}

impl<T> CoordinatedStateStore for Arc<T>
where
    T: CoordinatedStateStore + Send + Sync,
{
    type Error = T::Error;

    async fn load_or_initialize(
        &self,
        job_id: &str,
        initial_state: JobState,
    ) -> Result<CoordinatedRuntimeState, Self::Error> {
        self.as_ref()
            .load_or_initialize(job_id, initial_state)
            .await
    }

    async fn save_state(
        &self,
        job_id: &str,
        revision: u64,
        state: &JobState,
    ) -> Result<bool, Self::Error> {
        self.as_ref().save_state(job_id, revision, state).await
    }

    async fn reclaim_inflight(
        &self,
        job_id: &str,
        resource_id: &str,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        self.as_ref()
            .reclaim_inflight(job_id, resource_id, lease_config)
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
        self.as_ref()
            .claim_trigger(
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
        self.as_ref().renew(lease, lease_config).await
    }

    async fn complete(
        &self,
        job_id: &str,
        lease: &ExecutionLease,
        completion: CoordinatedCompletion,
    ) -> Result<bool, Self::Error> {
        self.as_ref().complete(job_id, lease, completion).await
    }

    async fn delete(&self, job_id: &str) -> Result<(), Self::Error> {
        self.as_ref().delete(job_id).await
    }

    async fn pause(&self, job_id: &str) -> Result<bool, Self::Error> {
        self.as_ref().pause(job_id).await
    }

    async fn resume(&self, job_id: &str) -> Result<bool, Self::Error> {
        self.as_ref().resume(job_id).await
    }

    fn classify_store_error(error: &Self::Error) -> StoreErrorKind
    where
        Self: Sized,
    {
        T::classify_store_error(error)
    }

    fn classify_guard_error(error: &Self::Error) -> ExecutionGuardErrorKind
    where
        Self: Sized,
    {
        T::classify_guard_error(error)
    }
}
