use super::coordinated_execution::{
    CoordinatedCompletedRun, apply_completed_coordinated_run, spawn_coordinated_trigger,
};
use super::engine::Scheduler;
use super::overlap::OverlapAction;
use super::runtime::{SchedulerRuntimeBackend, run_scheduler};
use super::runtime_events::{execution_guard_acquired, trigger_emitted};
use super::state_loading::{emit_state_loaded, emit_state_repaired, initial_runtime_state};
use super::trigger::PendingTrigger;
use crate::ExecutionGuard;
use crate::coordinated_store::{
    CoordinatedLeaseConfig, CoordinatedPendingTrigger, CoordinatedRuntimeState,
    CoordinatedStateStore,
};
use crate::error::SchedulerError;
use crate::error::{ExecutionGuardErrorKind, StoreErrorKind};
use crate::model::{Job, JobState, RunRecord};
use crate::observer::{PauseScope, StateLoadSource};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::task::JoinSet;

pub(super) async fn run_coordinated_scheduler<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    job: Job<D>,
    store: &Arc<C>,
    lease_config: CoordinatedLeaseConfig,
) -> Result<crate::SchedulerReport, SchedulerError>
where
    S: crate::StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    let backend = CoordinatedBackend {
        store: store.clone(),
        lease_config,
    };
    run_scheduler(scheduler, job, &backend).await
}

#[derive(Clone)]
struct CoordinatedBackend<C> {
    store: Arc<C>,
    lease_config: CoordinatedLeaseConfig,
}

impl<S, G, C, D> SchedulerRuntimeBackend<S, G, C, D> for CoordinatedBackend<C>
where
    S: crate::StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Runtime = CoordinatedRuntimeState;
    type Completed = CoordinatedCompletedRun;

    async fn initialize(
        &self,
        scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
    ) -> Result<CoordinatedRuntimeState, SchedulerError> {
        load_or_initialize_coordinated_state(scheduler, self.store.as_ref(), job, true).await
    }

    async fn refresh_runtime(
        &self,
        scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        _runtime: CoordinatedRuntimeState,
    ) -> Result<CoordinatedRuntimeState, SchedulerError> {
        match load_or_initialize_coordinated_state(scheduler, self.store.as_ref(), job, false).await
        {
            Ok(runtime) => Ok(runtime),
            Err(error) if is_store_connection_error(&error) => Ok(_runtime),
            Err(error) => Err(error),
        }
    }

    fn state<'a>(&self, runtime: &'a CoordinatedRuntimeState) -> &'a JobState {
        &runtime.state
    }

    fn is_paused(&self, runtime: &CoordinatedRuntimeState) -> bool {
        runtime.paused
    }

    fn pause_scope(&self) -> PauseScope {
        PauseScope::Shared
    }

    async fn save_state(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        runtime: &mut CoordinatedRuntimeState,
        state: &JobState,
    ) -> Result<bool, SchedulerError> {
        let saved = match self
            .store
            .save_state(&job.job_id, runtime.revision, state)
            .await
            .map_err(|error| {
                let kind = C::classify_store_error(&error);
                SchedulerError::store(error, kind)
            }) {
            Ok(saved) => saved,
            Err(error) if is_store_connection_error(&error) => return Ok(false),
            Err(error) => return Err(error),
        };
        if saved {
            runtime.revision += 1;
            runtime.state = state.clone();
        }
        Ok(saved)
    }

    async fn handle_queued_trigger(
        &self,
        scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        runtime: &mut CoordinatedRuntimeState,
        trigger: PendingTrigger,
        active: &mut JoinSet<CoordinatedCompletedRun>,
    ) -> Result<bool, SchedulerError> {
        let claim = match self
            .store
            .claim_trigger(
                &job.job_id,
                &job.execution_resource_id,
                runtime.revision,
                CoordinatedPendingTrigger {
                    scheduled_at: trigger.scheduled_at,
                    catch_up: trigger.catch_up,
                    trigger_count: trigger.trigger_count,
                    source: trigger.source,
                },
                &runtime.state,
                self.lease_config,
                job.guard_scope,
            )
            .await
            .map_err(|error| {
                let kind = C::classify_guard_error(&error);
                SchedulerError::execution_guard(error, kind)
            }) {
            Ok(claim) => claim,
            Err(error) if is_guard_connection_error(&error) => return Ok(false),
            Err(error) => return Err(error),
        };
        if let Some(claim) = claim {
            scheduler.emit(execution_guard_acquired(
                &claim.lease,
                trigger.catch_up,
                trigger.trigger_count,
            ));
            *runtime = claim.state.clone();
            spawn_coordinated_trigger(
                scheduler,
                self.store.clone(),
                self.lease_config,
                active,
                job,
                claim,
            )
            .await;
            return Ok(true);
        }
        Ok(false)
    }

    async fn try_reclaim_inflight(
        &self,
        scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        runtime: &mut CoordinatedRuntimeState,
        active: &mut JoinSet<CoordinatedCompletedRun>,
    ) -> Result<bool, SchedulerError> {
        let claim = match self
            .store
            .reclaim_inflight(&job.job_id, &job.execution_resource_id, self.lease_config)
            .await
            .map_err(|error| {
                let kind = C::classify_guard_error(&error);
                SchedulerError::execution_guard(error, kind)
            }) {
            Ok(claim) => claim,
            Err(error) if is_guard_connection_error(&error) => return Ok(false),
            Err(error) => return Err(error),
        };
        if let Some(claim) = claim {
            scheduler.emit(execution_guard_acquired(
                &claim.lease,
                claim.trigger.catch_up,
                claim.trigger.trigger_count,
            ));
            *runtime = claim.state.clone();
            spawn_coordinated_trigger(
                scheduler,
                self.store.clone(),
                self.lease_config,
                active,
                job,
                claim,
            )
            .await;
            return Ok(true);
        }
        Ok(false)
    }

    async fn handle_due_trigger(
        &self,
        scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        runtime: &mut CoordinatedRuntimeState,
        candidate_state: JobState,
        _trigger: PendingTrigger,
        overlap_action: OverlapAction,
        active: &mut JoinSet<CoordinatedCompletedRun>,
    ) -> Result<bool, SchedulerError> {
        match overlap_action {
            OverlapAction::Spawn(trigger) => {
                let claim = match self
                    .store
                    .claim_trigger(
                        &job.job_id,
                        &job.execution_resource_id,
                        runtime.revision,
                        CoordinatedPendingTrigger {
                            scheduled_at: trigger.scheduled_at,
                            catch_up: trigger.catch_up,
                            trigger_count: trigger.trigger_count,
                            source: trigger.source,
                        },
                        &candidate_state,
                        self.lease_config,
                        job.guard_scope,
                    )
                    .await
                    .map_err(|error| {
                        let kind = C::classify_guard_error(&error);
                        SchedulerError::execution_guard(error, kind)
                    }) {
                    Ok(claim) => claim,
                    Err(error) if is_guard_connection_error(&error) => return Ok(false),
                    Err(error) => return Err(error),
                };
                if let Some(claim) = claim {
                    *runtime = claim.state.clone();
                    scheduler.emit(trigger_emitted(
                        &job.job_id,
                        trigger.scheduled_at,
                        trigger.catch_up,
                        trigger.trigger_count,
                    ));
                    scheduler.emit(execution_guard_acquired(
                        &claim.lease,
                        trigger.catch_up,
                        trigger.trigger_count,
                    ));
                    spawn_coordinated_trigger(
                        scheduler,
                        self.store.clone(),
                        self.lease_config,
                        active,
                        job,
                        claim,
                    )
                    .await;
                    return Ok(true);
                }
                Ok(false)
            }
            OverlapAction::QueueUpdated | OverlapAction::Dropped => {
                let saved = match self
                    .store
                    .save_state(&job.job_id, runtime.revision, &candidate_state)
                    .await
                    .map_err(|error| {
                        let kind = C::classify_store_error(&error);
                        SchedulerError::store(error, kind)
                    }) {
                    Ok(saved) => saved,
                    Err(error) if is_store_connection_error(&error) => return Ok(false),
                    Err(error) => return Err(error),
                };
                if saved {
                    runtime.revision += 1;
                    runtime.state = candidate_state;
                }
                Ok(false)
            }
        }
    }

    async fn apply_completed(
        &self,
        scheduler: &Scheduler<S, G, C>,
        runtime: &mut CoordinatedRuntimeState,
        history: &mut VecDeque<RunRecord>,
        completed: CoordinatedCompletedRun,
    ) -> Result<(), SchedulerError> {
        match apply_completed_coordinated_run(
            scheduler,
            self.store.as_ref(),
            &mut runtime.state,
            history,
            completed,
        )
        .await
        {
            Ok(()) => {}
            Err(error) if is_store_connection_error(&error) => return Ok(()),
            Err(error) => return Err(error),
        }
        runtime.revision += 1;
        Ok(())
    }

    async fn delete_terminal_state(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        runtime: &mut CoordinatedRuntimeState,
    ) -> Result<(), SchedulerError> {
        match self.store.delete(&job.job_id).await.map_err(|error| {
            let kind = C::classify_store_error(&error);
            SchedulerError::store(error, kind)
        }) {
            Ok(()) => {}
            Err(error) if is_store_connection_error(&error) => return Ok(()),
            Err(error) => return Err(error),
        }
        runtime.state.next_run_at = None;
        Ok(())
    }

    async fn pause(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        _runtime: &mut CoordinatedRuntimeState,
    ) -> Result<bool, SchedulerError> {
        match self.store.pause(&job.job_id).await.map_err(|error| {
            let kind = C::classify_store_error(&error);
            SchedulerError::store(error, kind)
        }) {
            Ok(changed) => Ok(changed),
            Err(error) if is_store_connection_error(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn resume(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        _runtime: &mut CoordinatedRuntimeState,
    ) -> Result<bool, SchedulerError> {
        match self.store.resume(&job.job_id).await.map_err(|error| {
            let kind = C::classify_store_error(&error);
            SchedulerError::store(error, kind)
        }) {
            Ok(changed) => Ok(changed),
            Err(error) if is_store_connection_error(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

fn is_store_connection_error(error: &SchedulerError) -> bool {
    matches!(
        error,
        SchedulerError::Store(store_error) if store_error.kind() == StoreErrorKind::Connection
    )
}

fn is_guard_connection_error(error: &SchedulerError) -> bool {
    matches!(
        error,
        SchedulerError::ExecutionGuard(guard_error)
            if guard_error.kind() == ExecutionGuardErrorKind::Connection
    )
}

async fn load_or_initialize_coordinated_state<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    store: &C,
    job: &Job<D>,
    emit_load_event: bool,
) -> Result<CoordinatedRuntimeState, SchedulerError>
where
    S: crate::StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    let initial_state = initial_runtime_state(scheduler, job)?;
    let mut runtime = store
        .load_or_initialize(&job.job_id, initial_state)
        .await
        .map_err(|error| {
            let kind = C::classify_store_error(&error);
            SchedulerError::store(error, kind)
        })?;

    if scheduler.should_repair_interval_state(job, &runtime.state) {
        let previous_next_run_at = runtime.state.next_run_at;
        runtime.state.next_run_at =
            crate::scheduler::trigger_math::initial_next_run_at(job, scheduler.config.timezone)?;
        let saved = store
            .save_state(&job.job_id, runtime.revision, &runtime.state)
            .await
            .map_err(|error| {
                let kind = C::classify_store_error(&error);
                SchedulerError::store(error, kind)
            })?;
        if saved {
            runtime.revision += 1;
            emit_state_repaired(scheduler, &job.job_id, &runtime.state, previous_next_run_at);
            if emit_load_event {
                emit_state_loaded(
                    scheduler,
                    &job.job_id,
                    &runtime.state,
                    StateLoadSource::Repaired,
                );
            }
        }
    } else if emit_load_event {
        emit_state_loaded(
            scheduler,
            &job.job_id,
            &runtime.state,
            if runtime.revision == 0 {
                StateLoadSource::New
            } else {
                StateLoadSource::Restored
            },
        );
    }

    Ok(runtime)
}
