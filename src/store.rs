use crate::model::JobState;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::RwLock;

pub trait StateStore {
    fn load(&self, job_id: &str) -> impl Future<Output = Result<Option<JobState>, String>> + Send;
    fn save(&self, state: &JobState) -> impl Future<Output = Result<(), String>> + Send;
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
    async fn load(&self, job_id: &str) -> Result<Option<JobState>, String> {
        Ok(self.states.read().await.get(job_id).cloned())
    }

    async fn save(&self, state: &JobState) -> Result<(), String> {
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
    async fn load(&self, job_id: &str) -> Result<Option<JobState>, String> {
        self.as_ref().load(job_id).await
    }

    async fn save(&self, state: &JobState) -> Result<(), String> {
        self.as_ref().save(state).await
    }
}
