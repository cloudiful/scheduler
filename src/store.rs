use crate::model::JobState;
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::RwLock;

pub trait StateStore {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load(
        &self,
        job_id: &str,
    ) -> impl Future<Output = Result<Option<JobState>, Self::Error>> + Send;
    fn save(&self, state: &JobState) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug, Default)]
pub struct InMemoryStateStore {
    states: RwLock<HashMap<String, JobState>>,
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateStore for InMemoryStateStore {
    type Error = Infallible;

    async fn load(&self, job_id: &str) -> Result<Option<JobState>, Self::Error> {
        Ok(self.states.read().await.get(job_id).cloned())
    }

    async fn save(&self, state: &JobState) -> Result<(), Self::Error> {
        self.states
            .write()
            .await
            .insert(state.job_id.clone(), state.clone());
        Ok(())
    }
}

impl<T> StateStore for Arc<T>
where
    T: StateStore + Send + Sync + ?Sized,
{
    type Error = T::Error;

    async fn load(&self, job_id: &str) -> Result<Option<JobState>, Self::Error> {
        self.as_ref().load(job_id).await
    }

    async fn save(&self, state: &JobState) -> Result<(), Self::Error> {
        self.as_ref().save(state).await
    }
}
