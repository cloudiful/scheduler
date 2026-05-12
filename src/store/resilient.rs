use super::{InMemoryStateStore, ResilientStoreError, StateStore, StoreEvent, StoreOperation};
use crate::error::StoreErrorKind;
use crate::model::JobState;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;

/// Wraps a primary store with an in-process mirror that takes over after
/// connection-class failures and opportunistically writes dirty state back once
/// the primary accepts commands again.
#[derive(Debug)]
pub struct ResilientStateStore<S>
where
    S: StateStore,
    S::Error: ResilientStoreError,
{
    primary: S,
    degraded: AtomicBool,
    mirror: InMemoryStateStore,
    dirty: Mutex<HashMap<String, Option<JobState>>>,
    events: Mutex<VecDeque<StoreEvent>>,
}

impl<S> ResilientStateStore<S>
where
    S: StateStore,
    S::Error: ResilientStoreError,
{
    pub fn new(store: S) -> Self {
        Self {
            primary: store,
            degraded: AtomicBool::new(false),
            mirror: InMemoryStateStore::new(),
            dirty: Mutex::new(HashMap::new()),
            events: Mutex::new(VecDeque::new()),
        }
    }

    pub fn degraded(store: S) -> Self {
        Self {
            primary: store,
            degraded: AtomicBool::new(true),
            mirror: InMemoryStateStore::new(),
            dirty: Mutex::new(HashMap::new()),
            events: Mutex::new(VecDeque::new()),
        }
    }

    pub fn from_result(result: Result<S, S::Error>) -> Result<Self, S::Error> {
        match result {
            Ok(store) => Ok(Self::new(store)),
            Err(error) if error.is_connection_issue() => Err(error),
            Err(error) => Err(error),
        }
    }

    /// Returns true once the store has permanently fallen back to its
    /// in-process mirror.
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }

    async fn load_mirror(&self, job_id: &str) -> Result<Option<JobState>, S::Error> {
        match self.mirror.load(job_id).await {
            Ok(state) => Ok(state),
            Err(never) => match never {},
        }
    }

    async fn save_mirror(&self, state: &JobState) -> Result<(), S::Error> {
        match self.mirror.save(state).await {
            Ok(()) => Ok(()),
            Err(never) => match never {},
        }
    }

    async fn delete_mirror(&self, job_id: &str) -> Result<(), S::Error> {
        match self.mirror.delete(job_id).await {
            Ok(()) => Ok(()),
            Err(never) => match never {},
        }
    }

    async fn sync_mirror(&self, job_id: &str, state: Option<&JobState>) -> Result<(), S::Error> {
        match state {
            Some(state) => self.save_mirror(state).await,
            None => self.delete_mirror(job_id).await,
        }
    }

    async fn mark_degraded(&self, operation: StoreOperation, error: &S::Error) {
        let was_degraded = self.degraded.swap(true, Ordering::SeqCst);
        if !was_degraded {
            self.events.lock().await.push_back(StoreEvent::Degraded {
                operation,
                error: error.to_string(),
            });
        }
    }

    async fn mark_recovering(&self, operation: StoreOperation) {
        self.events
            .lock()
            .await
            .push_back(StoreEvent::Recovering { operation });
    }

    async fn mark_recovered(&self, operation: StoreOperation) {
        let was_degraded = self.degraded.swap(false, Ordering::SeqCst);
        if was_degraded {
            self.events
                .lock()
                .await
                .push_back(StoreEvent::Recovered { operation });
        }
    }

    async fn mark_recovery_failed(&self, operation: StoreOperation, error: &S::Error) {
        self.events
            .lock()
            .await
            .push_back(StoreEvent::RecoveryFailed {
                operation,
                error: error.to_string(),
            });
    }

    async fn record_dirty(&self, job_id: String, state: Option<JobState>) {
        self.dirty.lock().await.insert(job_id, state);
    }

    async fn clear_dirty(&self, job_id: &str, operation: StoreOperation) {
        let mut dirty = self.dirty.lock().await;
        dirty.remove(job_id);
        if dirty.is_empty() {
            drop(dirty);
            self.mark_recovered(operation).await;
        }
    }

    async fn dirty_state(&self, job_id: &str) -> Option<Option<JobState>> {
        self.dirty.lock().await.get(job_id).cloned()
    }
}

impl<S> StateStore for ResilientStateStore<S>
where
    S: StateStore + Send + Sync,
    S::Error: ResilientStoreError,
{
    type Error = S::Error;

    async fn load(&self, job_id: &str) -> Result<Option<JobState>, Self::Error> {
        if self.is_degraded() {
            if let Some(dirty) = self.dirty_state(job_id).await {
                return Ok(dirty);
            }
        }

        match self.primary.load(job_id).await {
            Ok(state) => {
                self.sync_mirror(job_id, state.as_ref()).await?;
                if self.is_degraded() {
                    self.mark_recovered(StoreOperation::Load).await;
                }
                Ok(state)
            }
            Err(error) if error.is_connection_issue() => {
                self.mark_degraded(StoreOperation::Load, &error).await;
                self.load_mirror(job_id).await
            }
            Err(error) => Err(error),
        }
    }

    async fn save(&self, state: &JobState) -> Result<(), Self::Error> {
        self.save_mirror(state).await?;
        if self.is_degraded() {
            self.record_dirty(state.job_id.clone(), Some(state.clone()))
                .await;
            self.mark_recovering(StoreOperation::Save).await;
        }

        match self.primary.save(state).await {
            Ok(()) => {
                self.clear_dirty(&state.job_id, StoreOperation::Save).await;
                Ok(())
            }
            Err(error) if error.is_connection_issue() => {
                self.mark_degraded(StoreOperation::Save, &error).await;
                self.record_dirty(state.job_id.clone(), Some(state.clone()))
                    .await;
                if self.is_degraded() {
                    self.mark_recovery_failed(StoreOperation::Save, &error)
                        .await;
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn delete(&self, job_id: &str) -> Result<(), Self::Error> {
        self.delete_mirror(job_id).await?;
        if self.is_degraded() {
            self.record_dirty(job_id.to_string(), None).await;
            self.mark_recovering(StoreOperation::Delete).await;
        }

        match self.primary.delete(job_id).await {
            Ok(()) => {
                self.clear_dirty(job_id, StoreOperation::Delete).await;
                Ok(())
            }
            Err(error) if error.is_connection_issue() => {
                self.mark_degraded(StoreOperation::Delete, &error).await;
                self.record_dirty(job_id.to_string(), None).await;
                if self.is_degraded() {
                    self.mark_recovery_failed(StoreOperation::Delete, &error)
                        .await;
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn drain_events(&self) -> Result<Vec<StoreEvent>, Self::Error> {
        let mut events = self.events.lock().await;
        Ok(events.drain(..).collect())
    }

    fn classify_error(error: &Self::Error) -> StoreErrorKind
    where
        Self: Sized,
    {
        S::classify_error(error)
    }
}
