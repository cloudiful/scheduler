use crate::error::ExecutionGuardErrorKind;
use chrono::{DateTime, Utc};
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionGuardScope {
    #[default]
    Occurrence,
    Resource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSlot {
    pub job_id: String,
    pub resource_id: String,
    pub scope: ExecutionGuardScope,
    pub scheduled_at: Option<DateTime<Utc>>,
}

impl ExecutionSlot {
    pub fn new(job_id: impl Into<String>, scheduled_at: DateTime<Utc>) -> Self {
        let job_id = job_id.into();
        Self::for_occurrence(job_id.clone(), job_id, scheduled_at)
    }

    pub fn for_occurrence(
        job_id: impl Into<String>,
        resource_id: impl Into<String>,
        scheduled_at: DateTime<Utc>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            resource_id: resource_id.into(),
            scope: ExecutionGuardScope::Occurrence,
            scheduled_at: Some(scheduled_at),
        }
    }

    pub fn for_resource(job_id: impl Into<String>, resource_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
            resource_id: resource_id.into(),
            scope: ExecutionGuardScope::Resource,
            scheduled_at: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionLease {
    pub job_id: String,
    pub resource_id: String,
    pub scope: ExecutionGuardScope,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub token: String,
    pub lease_key: String,
}

impl ExecutionLease {
    pub fn new(
        job_id: impl Into<String>,
        resource_id: impl Into<String>,
        scope: ExecutionGuardScope,
        scheduled_at: Option<DateTime<Utc>>,
        token: impl Into<String>,
        lease_key: impl Into<String>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            resource_id: resource_id.into(),
            scope,
            scheduled_at,
            token: token.into(),
            lease_key: lease_key.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionGuardAcquire {
    Acquired(ExecutionLease),
    Contended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionGuardRenewal {
    Renewed,
    Lost,
}

pub trait ExecutionGuard {
    type Error: std::error::Error + Send + Sync + 'static;

    fn acquire(
        &self,
        slot: ExecutionSlot,
    ) -> impl Future<Output = Result<ExecutionGuardAcquire, Self::Error>> + Send;

    fn renew(
        &self,
        lease: &ExecutionLease,
    ) -> impl Future<Output = Result<ExecutionGuardRenewal, Self::Error>> + Send;

    fn release(
        &self,
        lease: &ExecutionLease,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn classify_error(_error: &Self::Error) -> ExecutionGuardErrorKind
    where
        Self: Sized,
    {
        ExecutionGuardErrorKind::Unknown
    }

    fn renew_interval(&self, _lease: &ExecutionLease) -> Option<Duration> {
        None
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopExecutionGuard;

impl ExecutionGuard for NoopExecutionGuard {
    type Error = Infallible;

    async fn acquire(&self, slot: ExecutionSlot) -> Result<ExecutionGuardAcquire, Self::Error> {
        Ok(ExecutionGuardAcquire::Acquired(ExecutionLease::new(
            slot.job_id,
            slot.resource_id,
            slot.scope,
            slot.scheduled_at,
            "",
            "",
        )))
    }

    async fn renew(&self, _lease: &ExecutionLease) -> Result<ExecutionGuardRenewal, Self::Error> {
        Ok(ExecutionGuardRenewal::Renewed)
    }

    async fn release(&self, _lease: &ExecutionLease) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<T> ExecutionGuard for Arc<T>
where
    T: ExecutionGuard + Send + Sync,
{
    type Error = T::Error;

    async fn acquire(&self, slot: ExecutionSlot) -> Result<ExecutionGuardAcquire, Self::Error> {
        self.as_ref().acquire(slot).await
    }

    async fn renew(&self, lease: &ExecutionLease) -> Result<ExecutionGuardRenewal, Self::Error> {
        self.as_ref().renew(lease).await
    }

    async fn release(&self, lease: &ExecutionLease) -> Result<(), Self::Error> {
        self.as_ref().release(lease).await
    }

    fn classify_error(error: &Self::Error) -> ExecutionGuardErrorKind
    where
        Self: Sized,
    {
        T::classify_error(error)
    }

    fn renew_interval(&self, lease: &ExecutionLease) -> Option<Duration> {
        self.as_ref().renew_interval(lease)
    }
}
