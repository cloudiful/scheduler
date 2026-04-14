use super::{InMemoryStateStore, ResilientStoreError, StateStore, StoreEvent, StoreOperation};
use crate::error::StoreErrorKind;
use crate::model::JobState;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;

#[derive(Debug)]
enum ResilientMode<S> {
    Primary(S),
    MemoryOnly,
}

/// Wraps a primary store with an in-process mirror that takes over after
/// connection-class failures.
#[derive(Debug)]
pub struct ResilientStateStore<S>
where
    S: StateStore,
    S::Error: ResilientStoreError,
{
    mode: Mutex<ResilientMode<S>>,
    degraded: AtomicBool,
    mirror: InMemoryStateStore,
    events: Mutex<VecDeque<StoreEvent>>,
}

impl<S> ResilientStateStore<S>
where
    S: StateStore,
    S::Error: ResilientStoreError,
{
    pub fn new(store: S) -> Self {
        Self {
            mode: Mutex::new(ResilientMode::Primary(store)),
            degraded: AtomicBool::new(false),
            mirror: InMemoryStateStore::new(),
            events: Mutex::new(VecDeque::new()),
        }
    }

    pub fn degraded() -> Self {
        Self {
            mode: Mutex::new(ResilientMode::MemoryOnly),
            degraded: AtomicBool::new(true),
            mirror: InMemoryStateStore::new(),
            events: Mutex::new(VecDeque::new()),
        }
    }

    pub fn from_result(result: Result<S, S::Error>) -> Result<Self, S::Error> {
        match result {
            Ok(store) => Ok(Self::new(store)),
            Err(error) if error.is_connection_issue() => Ok(Self::degraded()),
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
}

impl<S> StateStore for ResilientStateStore<S>
where
    S: StateStore + Send,
    S::Error: ResilientStoreError,
{
    type Error = S::Error;

    async fn load(&self, job_id: &str) -> Result<Option<JobState>, Self::Error> {
        let mut mode = self.mode.lock().await;

        match &mut *mode {
            ResilientMode::Primary(store) => match store.load(job_id).await {
                Ok(state) => {
                    drop(mode);
                    self.sync_mirror(job_id, state.as_ref()).await?;
                    Ok(state)
                }
                Err(error) if error.is_connection_issue() => {
                    drop(mode);
                    self.mark_degraded(StoreOperation::Load, &error).await;
                    let mut mode = self.mode.lock().await;
                    *mode = ResilientMode::MemoryOnly;
                    drop(mode);
                    self.load_mirror(job_id).await
                }
                Err(error) => Err(error),
            },
            ResilientMode::MemoryOnly => {
                drop(mode);
                self.load_mirror(job_id).await
            }
        }
    }

    async fn save(&self, state: &JobState) -> Result<(), Self::Error> {
        let mut mode = self.mode.lock().await;

        match &mut *mode {
            ResilientMode::Primary(store) => match store.save(state).await {
                Ok(()) => {
                    drop(mode);
                    self.save_mirror(state).await
                }
                Err(error) if error.is_connection_issue() => {
                    drop(mode);
                    self.mark_degraded(StoreOperation::Save, &error).await;
                    self.save_mirror(state).await?;
                    let mut mode = self.mode.lock().await;
                    *mode = ResilientMode::MemoryOnly;
                    Ok(())
                }
                Err(error) => Err(error),
            },
            ResilientMode::MemoryOnly => {
                drop(mode);
                self.save_mirror(state).await
            }
        }
    }

    async fn delete(&self, job_id: &str) -> Result<(), Self::Error> {
        let mut mode = self.mode.lock().await;

        match &mut *mode {
            ResilientMode::Primary(store) => match store.delete(job_id).await {
                Ok(()) => {
                    drop(mode);
                    self.delete_mirror(job_id).await
                }
                Err(error) if error.is_connection_issue() => {
                    drop(mode);
                    self.mark_degraded(StoreOperation::Delete, &error).await;
                    self.delete_mirror(job_id).await?;
                    let mut mode = self.mode.lock().await;
                    *mode = ResilientMode::MemoryOnly;
                    Ok(())
                }
                Err(error) => Err(error),
            },
            ResilientMode::MemoryOnly => {
                drop(mode);
                self.delete_mirror(job_id).await
            }
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
