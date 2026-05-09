use super::coordinated_execution::{
    CoordinatedCompletedRun, apply_completed_coordinated_run, spawn_coordinated_trigger,
};
use super::engine::Scheduler;
use super::overlap::OverlapAction;
use super::runtime::{SchedulerRuntimeBackend, run_scheduler};
use super::trigger::PendingTrigger;
use crate::ExecutionGuard;
use crate::coordinated_store::{
    CoordinatedLeaseConfig, CoordinatedPendingTrigger, CoordinatedRuntimeState,
    CoordinatedStateStore,
};
use crate::error::SchedulerError;
use crate::model::{Job, JobState, RunRecord};
use crate::observer::{PauseScope, SchedulerEvent, StateLoadSource};
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
        load_or_initialize_coordinated_state(scheduler, self.store.as_ref(), job, false).await
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
        let saved = self
            .store
            .save_state(&job.job_id, runtime.revision, state)
            .await
            .map_err(|error| {
                let kind = C::classify_store_error(&error);
                SchedulerError::store(error, kind)
            })?;
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
        let claim = self
            .store
            .claim_trigger(
                &job.job_id,
                &job.execution_resource_id,
                runtime.revision,
                CoordinatedPendingTrigger {
                    scheduled_at: trigger.scheduled_at,
                    catch_up: trigger.catch_up,
                    trigger_count: trigger.trigger_count,
                },
                &runtime.state,
                self.lease_config,
            )
            .await
            .map_err(|error| {
                let kind = C::classify_guard_error(&error);
                SchedulerError::execution_guard(error, kind)
            })?;
        if let Some(claim) = claim {
            scheduler.emit(SchedulerEvent::ExecutionGuardAcquired {
                job_id: claim.lease.job_id.clone(),
                resource_id: claim.lease.resource_id.clone(),
                scope: claim.lease.scope,
                lease_key: claim.lease.lease_key.clone(),
                scheduled_at: claim.lease.scheduled_at,
                catch_up: trigger.catch_up,
                trigger_count: trigger.trigger_count,
            });
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
        if let Some(claim) = self
            .store
            .reclaim_inflight(&job.job_id, &job.execution_resource_id, self.lease_config)
            .await
            .map_err(|error| {
                let kind = C::classify_guard_error(&error);
                SchedulerError::execution_guard(error, kind)
            })?
        {
            scheduler.emit(SchedulerEvent::ExecutionGuardAcquired {
                job_id: claim.lease.job_id.clone(),
                resource_id: claim.lease.resource_id.clone(),
                scope: claim.lease.scope,
                lease_key: claim.lease.lease_key.clone(),
                scheduled_at: claim.lease.scheduled_at,
                catch_up: claim.trigger.catch_up,
                trigger_count: claim.trigger.trigger_count,
            });
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
                let claim = self
                    .store
                    .claim_trigger(
                        &job.job_id,
                        &job.execution_resource_id,
                        runtime.revision,
                        CoordinatedPendingTrigger {
                            scheduled_at: trigger.scheduled_at,
                            catch_up: trigger.catch_up,
                            trigger_count: trigger.trigger_count,
                        },
                        &candidate_state,
                        self.lease_config,
                    )
                    .await
                    .map_err(|error| {
                        let kind = C::classify_guard_error(&error);
                        SchedulerError::execution_guard(error, kind)
                    })?;
                if let Some(claim) = claim {
                    *runtime = claim.state.clone();
                    scheduler.emit(SchedulerEvent::TriggerEmitted {
                        job_id: job.job_id.clone(),
                        scheduled_at: trigger.scheduled_at,
                        catch_up: trigger.catch_up,
                        trigger_count: trigger.trigger_count,
                    });
                    scheduler.emit(SchedulerEvent::ExecutionGuardAcquired {
                        job_id: claim.lease.job_id.clone(),
                        resource_id: claim.lease.resource_id.clone(),
                        scope: claim.lease.scope,
                        lease_key: claim.lease.lease_key.clone(),
                        scheduled_at: claim.lease.scheduled_at,
                        catch_up: trigger.catch_up,
                        trigger_count: trigger.trigger_count,
                    });
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
                let saved = self
                    .store
                    .save_state(&job.job_id, runtime.revision, &candidate_state)
                    .await
                    .map_err(|error| {
                        let kind = C::classify_store_error(&error);
                        SchedulerError::store(error, kind)
                    })?;
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
        apply_completed_coordinated_run(
            scheduler,
            self.store.as_ref(),
            &mut runtime.state,
            history,
            completed,
        )
        .await?;
        runtime.revision += 1;
        Ok(())
    }

    async fn delete_terminal_state(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        runtime: &mut CoordinatedRuntimeState,
    ) -> Result<(), SchedulerError> {
        self.store.delete(&job.job_id).await.map_err(|error| {
            let kind = C::classify_store_error(&error);
            SchedulerError::store(error, kind)
        })?;
        runtime.state.next_run_at = None;
        Ok(())
    }

    async fn pause(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        _runtime: &mut CoordinatedRuntimeState,
    ) -> Result<bool, SchedulerError> {
        self.store.pause(&job.job_id).await.map_err(|error| {
            let kind = C::classify_store_error(&error);
            SchedulerError::store(error, kind)
        })
    }

    async fn resume(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        _runtime: &mut CoordinatedRuntimeState,
    ) -> Result<bool, SchedulerError> {
        self.store.resume(&job.job_id).await.map_err(|error| {
            let kind = C::classify_store_error(&error);
            SchedulerError::store(error, kind)
        })
    }
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
    let initial_state = JobState::new(
        job.job_id.clone(),
        crate::scheduler::trigger_math::initial_next_run_at(job, scheduler.config.timezone)?,
    );
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
            scheduler.emit(SchedulerEvent::StateRepaired {
                job_id: job.job_id.clone(),
                trigger_count: runtime.state.trigger_count,
                previous_next_run_at,
                repaired_next_run_at: runtime.state.next_run_at,
            });
            if emit_load_event {
                scheduler.emit(SchedulerEvent::StateLoaded {
                    job_id: job.job_id.clone(),
                    trigger_count: runtime.state.trigger_count,
                    next_run_at: runtime.state.next_run_at,
                    source: StateLoadSource::Repaired,
                });
            }
        }
    } else if emit_load_event {
        scheduler.emit(SchedulerEvent::StateLoaded {
            job_id: job.job_id.clone(),
            trigger_count: runtime.state.trigger_count,
            next_run_at: runtime.state.next_run_at,
            source: if runtime.revision == 0 {
                StateLoadSource::New
            } else {
                StateLoadSource::Restored
            },
        });
    }

    Ok(runtime)
}
