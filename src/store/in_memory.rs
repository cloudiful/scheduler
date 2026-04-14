use super::StateStore;
use crate::model::JobState;
use std::collections::HashMap;
use std::convert::Infallible;
use tokio::sync::RwLock;

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

    async fn delete(&self, job_id: &str) -> Result<(), Self::Error> {
        self.states.write().await.remove(job_id);
        Ok(())
    }
}
