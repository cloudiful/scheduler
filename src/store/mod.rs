mod in_memory;
mod resilient;

use crate::error::StoreErrorKind;
use crate::model::JobState;
use std::convert::Infallible;
use std::error::Error;
use std::future::{Future, ready};
use std::sync::Arc;

pub use in_memory::InMemoryStateStore;
pub use resilient::ResilientStateStore;

pub trait StateStore {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load(
        &self,
        job_id: &str,
    ) -> impl Future<Output = Result<Option<JobState>, Self::Error>> + Send;

    fn save(&self, state: &JobState) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn delete(&self, _job_id: &str) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn drain_events(&self) -> impl Future<Output = Result<Vec<StoreEvent>, Self::Error>> + Send {
        ready(Ok(Vec::new()))
    }

    fn classify_error(_error: &Self::Error) -> StoreErrorKind
    where
        Self: Sized,
    {
        StoreErrorKind::Unknown
    }
}

/// Classifies store errors that should trigger a one-way downgrade to the
/// in-process mirror store.
pub trait ResilientStoreError: Error + Send + Sync + 'static {
    fn is_connection_issue(&self) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOperation {
    Load,
    Save,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreEvent {
    Degraded {
        operation: StoreOperation,
        error: String,
    },
    Recovering {
        operation: StoreOperation,
    },
    Recovered {
        operation: StoreOperation,
    },
    RecoveryFailed {
        operation: StoreOperation,
        error: String,
    },
    RecoveryConflict {
        operation: StoreOperation,
        job_id: String,
        error: String,
    },
}

impl<T> StateStore for Arc<T>
where
    T: StateStore + Send + Sync,
{
    type Error = T::Error;

    async fn load(&self, job_id: &str) -> Result<Option<JobState>, Self::Error> {
        self.as_ref().load(job_id).await
    }

    async fn save(&self, state: &JobState) -> Result<(), Self::Error> {
        self.as_ref().save(state).await
    }

    async fn delete(&self, job_id: &str) -> Result<(), Self::Error> {
        self.as_ref().delete(job_id).await
    }

    async fn drain_events(&self) -> Result<Vec<StoreEvent>, Self::Error> {
        self.as_ref().drain_events().await
    }

    fn classify_error(error: &Self::Error) -> StoreErrorKind
    where
        Self: Sized,
    {
        T::classify_error(error)
    }
}

impl ResilientStoreError for Infallible {
    fn is_connection_issue(&self) -> bool {
        match *self {}
    }
}
