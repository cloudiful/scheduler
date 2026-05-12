use super::control::{ControlSignal, PauseController, SchedulerHandle};
use super::coordinated::run_coordinated_scheduler;
use super::legacy::run_legacy_scheduler;
use crate::coordinated_store::{
    CoordinatedLeaseConfig, CoordinatedStateStore, NoopCoordinatedStateStore,
};
use crate::error::SchedulerError;
use crate::model::{Job, JobState, SchedulerConfig};
use crate::observer::{LogObserver, NoopObserver, SchedulerEvent, SchedulerObserver};
use crate::store::{StateStore, StoreEvent};
use crate::{ExecutionGuard, InMemoryStateStore, NoopExecutionGuard};
use chrono::Utc;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;

pub(super) enum SchedulerBackend<S, G, C>
where
    S: StateStore,
    G: ExecutionGuard,
    C: CoordinatedStateStore,
{
    Legacy {
        store: Arc<S>,
        guard: Arc<G>,
    },
    Coordinated {
        store: Arc<C>,
        lease_config: CoordinatedLeaseConfig,
    },
}

pub struct Scheduler<S, G = NoopExecutionGuard, C = NoopCoordinatedStateStore>
where
    S: StateStore,
    G: ExecutionGuard,
    C: CoordinatedStateStore,
{
    pub(super) config: SchedulerConfig,
    pub(super) backend: SchedulerBackend<S, G, C>,
    pub(super) observer: Arc<dyn SchedulerObserver>,
    pub(super) control: watch::Sender<ControlSignal>,
    pub(super) active_job_ids: Arc<Mutex<HashSet<String>>>,
}

impl<S> Scheduler<S, NoopExecutionGuard, NoopCoordinatedStateStore>
where
    S: StateStore + Send + Sync + 'static,
{
    pub fn new(config: SchedulerConfig, store: S) -> Self {
        Self::with_observer_and_execution_guard(config, store, NoopObserver, NoopExecutionGuard)
    }

    pub fn with_log_observer(config: SchedulerConfig, store: S) -> Self {
        Self::with_observer_and_execution_guard(config, store, LogObserver, NoopExecutionGuard)
    }

    pub fn with_observer<O>(config: SchedulerConfig, store: S, observer: O) -> Self
    where
        O: SchedulerObserver,
    {
        Self::with_observer_and_execution_guard(config, store, observer, NoopExecutionGuard)
    }
}

impl<S, G> Scheduler<S, G, NoopCoordinatedStateStore>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
{
    pub fn with_execution_guard(config: SchedulerConfig, store: S, guard: G) -> Self {
        Self::with_observer_and_execution_guard(config, store, NoopObserver, guard)
    }

    pub fn with_observer_and_execution_guard<O>(
        config: SchedulerConfig,
        store: S,
        observer: O,
        guard: G,
    ) -> Self
    where
        O: SchedulerObserver,
    {
        let (control, _) = watch::channel(ControlSignal::running());
        Self {
            config,
            backend: SchedulerBackend::Legacy {
                store: Arc::new(store),
                guard: Arc::new(guard),
            },
            observer: Arc::new(observer),
            control,
            active_job_ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl<C> Scheduler<InMemoryStateStore, NoopExecutionGuard, C>
where
    C: CoordinatedStateStore + Send + Sync + 'static,
{
    pub fn with_coordinated_state_store(
        config: SchedulerConfig,
        store: C,
        lease_config: CoordinatedLeaseConfig,
    ) -> Self {
        Self::with_observer_and_coordinated_state_store(config, store, NoopObserver, lease_config)
    }

    pub fn with_observer_and_coordinated_state_store<O>(
        config: SchedulerConfig,
        store: C,
        observer: O,
        lease_config: CoordinatedLeaseConfig,
    ) -> Self
    where
        O: SchedulerObserver,
    {
        let (control, _) = watch::channel(ControlSignal::running());
        Self {
            config,
            backend: SchedulerBackend::Coordinated {
                store: Arc::new(store),
                lease_config,
            },
            observer: Arc::new(observer),
            control,
            active_job_ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl<S, G, C> Scheduler<S, G, C>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: CoordinatedStateStore + Send + Sync + 'static,
{
    fn pause_controller(&self) -> Option<Arc<dyn PauseController>> {
        match &self.backend {
            SchedulerBackend::Legacy { .. } => None,
            SchedulerBackend::Coordinated { store, .. } => {
                Some(Arc::new(CoordinatedPauseController {
                    store: store.clone(),
                }))
            }
        }
    }

    pub(super) fn emit(&self, event: SchedulerEvent) {
        self.observer.on_event(&event);
    }

    pub(super) fn should_repair_interval_state<D>(&self, job: &Job<D>, state: &JobState) -> bool
    where
        D: Send + Sync + 'static,
    {
        if state.next_run_at.is_some() {
            return false;
        }
        if !matches!(
            job.schedule,
            crate::Schedule::Interval(_)
                | crate::Schedule::StaggeredInterval(_)
                | crate::Schedule::GroupedInterval(_)
        ) {
            return false;
        }
        match job.max_runs {
            None => true,
            Some(max_runs) => state.trigger_count < max_runs,
        }
    }

    pub(super) fn should_wait_for_active_replay<D>(&self, job: &Job<D>, active_count: usize) -> bool
    where
        D: Send + Sync + 'static,
    {
        active_count > 0
            && matches!(job.missed_run_policy, crate::MissedRunPolicy::ReplayAll)
            && !matches!(job.overlap_policy, crate::OverlapPolicy::AllowParallel)
    }

    pub(super) async fn sleep_until_next(&self, next_run_at: Option<chrono::DateTime<Utc>>) {
        let Some(next_run_at) = next_run_at else {
            return;
        };
        let now = Utc::now();
        if let Ok(duration) = (next_run_at - now).to_std() {
            tokio::time::sleep(duration).await;
        }
    }

    pub(super) async fn emit_store_events_for(
        &self,
        store: &S,
        job_id: &str,
    ) -> Result<(), SchedulerError> {
        let events = store.drain_events().await.map_err(|error| {
            let kind = S::classify_error(&error);
            SchedulerError::store(error, kind)
        })?;

        for event in events {
            match event {
                StoreEvent::Degraded { operation, error } => {
                    self.emit(SchedulerEvent::StoreDegraded {
                        job_id: job_id.to_string(),
                        operation,
                        error,
                    });
                }
                StoreEvent::Recovering { operation } => {
                    self.emit(SchedulerEvent::StoreRecovering {
                        job_id: job_id.to_string(),
                        operation,
                    });
                }
                StoreEvent::Recovered { operation } => {
                    self.emit(SchedulerEvent::StoreRecovered {
                        job_id: job_id.to_string(),
                        operation,
                    });
                }
                StoreEvent::RecoveryFailed { operation, error } => {
                    self.emit(SchedulerEvent::StoreRecoveryFailed {
                        job_id: job_id.to_string(),
                        operation,
                        error,
                    });
                }
                StoreEvent::RecoveryConflict {
                    operation,
                    job_id,
                    error,
                } => {
                    self.emit(SchedulerEvent::StoreRecoveryConflict {
                        job_id,
                        operation,
                        error,
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) async fn load_state_from_legacy(
        &self,
        store: &S,
        job_id: &str,
    ) -> Result<Option<JobState>, SchedulerError> {
        let state = store.load(job_id).await.map_err(|error| {
            let kind = S::classify_error(&error);
            SchedulerError::store(error, kind)
        })?;
        self.emit_store_events_for(store, job_id).await?;
        Ok(state)
    }

    pub(super) async fn persist_state_to_legacy(
        &self,
        store: &S,
        state: &JobState,
    ) -> Result<(), SchedulerError> {
        store.save(state).await.map_err(|error| {
            let kind = S::classify_error(&error);
            SchedulerError::store(error, kind)
        })?;
        self.emit_store_events_for(store, &state.job_id).await
    }

    pub(super) async fn delete_state_from_legacy(
        &self,
        store: &S,
        job_id: &str,
    ) -> Result<(), SchedulerError> {
        store.delete(job_id).await.map_err(|error| {
            let kind = S::classify_error(&error);
            SchedulerError::store(error, kind)
        })?;
        self.emit_store_events_for(store, job_id).await
    }

    fn normalize_job<D>(&self, mut job: Job<D>) -> Result<Job<D>, SchedulerError>
    where
        D: Send + Sync + 'static,
    {
        match &mut job.schedule {
            crate::Schedule::Interval(every) => {
                if every.is_zero() {
                    return Err(SchedulerError::invalid_zero_interval());
                }
            }
            crate::Schedule::StaggeredInterval(staggered) => {
                if staggered.every.is_zero() {
                    return Err(SchedulerError::invalid_zero_interval());
                }
            }
            crate::Schedule::GroupedInterval(grouped) => {
                if grouped.every.is_zero() {
                    return Err(SchedulerError::invalid_zero_interval());
                }
                crate::scheduler::trigger_math::validate_grouped_interval(grouped)?;
            }
            crate::Schedule::AtTimes(times) => times.sort_unstable(),
            crate::Schedule::Cron(_) => {}
        }

        if matches!(self.backend, SchedulerBackend::Coordinated { .. })
            && matches!(job.overlap_policy, crate::OverlapPolicy::AllowParallel)
        {
            return Err(SchedulerError::invalid_job_with_kind(
                crate::InvalidJobKind::Other,
                "coordinated scheduler does not support OverlapPolicy::AllowParallel",
            ));
        }

        Ok(job)
    }

    pub fn handle(&self) -> SchedulerHandle {
        SchedulerHandle::new(
            self.control.clone(),
            self.pause_controller(),
            self.active_job_ids.clone(),
        )
    }

    pub async fn run<D>(&self, job: Job<D>) -> Result<crate::SchedulerReport, SchedulerError>
    where
        D: Send + Sync + 'static,
    {
        let job = self.normalize_job(job)?;
        let job_id = job.job_id.clone();
        self.active_job_ids.lock().unwrap().insert(job_id.clone());
        let result = match &self.backend {
            SchedulerBackend::Legacy { store, guard } => {
                run_legacy_scheduler(self, job, store, guard).await
            }
            SchedulerBackend::Coordinated {
                store,
                lease_config,
            } => run_coordinated_scheduler(self, job, store, *lease_config).await,
        };
        self.active_job_ids.lock().unwrap().remove(&job_id);
        result
    }
}

struct CoordinatedPauseController<C> {
    store: Arc<C>,
}

impl<C> PauseController for CoordinatedPauseController<C>
where
    C: CoordinatedStateStore + Send + Sync + 'static,
{
    fn pause<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, SchedulerError>> + Send + 'a>> {
        Box::pin(async move {
            self.store.pause(job_id).await.map_err(|error| {
                let kind = C::classify_store_error(&error);
                SchedulerError::store(error, kind)
            })
        })
    }

    fn resume<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, SchedulerError>> + Send + 'a>> {
        Box::pin(async move {
            self.store.resume(job_id).await.map_err(|error| {
                let kind = C::classify_store_error(&error);
                SchedulerError::store(error, kind)
            })
        })
    }
}
