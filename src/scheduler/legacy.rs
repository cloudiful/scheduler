use super::engine::Scheduler;
use super::execution::{CompletedRun, apply_completed_run, spawn_legacy_trigger};
use super::overlap::OverlapAction;
use super::runtime::{SchedulerRuntimeBackend, run_scheduler};
use super::runtime_events::{execution_guard_acquired, run_completed, trigger_emitted};
use super::state_loading::{emit_state_loaded, emit_state_repaired, initial_runtime_state};
use super::trigger::PendingTrigger;
use crate::error::SchedulerError;
use crate::model::{Job, JobState, RunRecord};
use crate::observer::{PauseScope, SchedulerEvent, StateLoadSource};
use crate::store::StateStore;
use crate::{ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardScope, ExecutionSlot};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::task::JoinSet;

pub(super) async fn run_legacy_scheduler<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    job: Job<D>,
    store: &Arc<S>,
    guard: &Arc<G>,
) -> Result<crate::SchedulerReport, SchedulerError>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    let backend = LegacyBackend {
        store: store.clone(),
        guard: guard.clone(),
    };
    run_scheduler(scheduler, job, &backend).await
}

#[derive(Clone)]
struct LegacyBackend<S, G> {
    store: Arc<S>,
    guard: Arc<G>,
}

struct LegacyRuntime {
    state: JobState,
    paused: bool,
}

impl<S, G, C, D> SchedulerRuntimeBackend<S, G, C, D> for LegacyBackend<S, G>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    type Runtime = LegacyRuntime;
    type Completed = CompletedRun;

    async fn initialize(
        &self,
        scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
    ) -> Result<LegacyRuntime, SchedulerError> {
        let (state, state_is_new) =
            load_or_initialize_legacy_state(scheduler, self.store.as_ref(), job).await?;
        if state_is_new {
            scheduler
                .persist_state_to_legacy(self.store.as_ref(), &state)
                .await?;
        }
        Ok(LegacyRuntime {
            state,
            paused: false,
        })
    }

    async fn refresh_runtime(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        _job: &Job<D>,
        runtime: LegacyRuntime,
    ) -> Result<LegacyRuntime, SchedulerError> {
        Ok(runtime)
    }

    fn state<'a>(&self, runtime: &'a LegacyRuntime) -> &'a JobState {
        &runtime.state
    }

    fn is_paused(&self, runtime: &LegacyRuntime) -> bool {
        runtime.paused
    }

    fn pause_scope(&self) -> PauseScope {
        PauseScope::Local
    }

    async fn save_state(
        &self,
        scheduler: &Scheduler<S, G, C>,
        _job: &Job<D>,
        runtime: &mut LegacyRuntime,
        state: &JobState,
    ) -> Result<bool, SchedulerError> {
        runtime.state = state.clone();
        scheduler
            .persist_state_to_legacy(self.store.as_ref(), &runtime.state)
            .await?;
        Ok(true)
    }

    async fn handle_queued_trigger(
        &self,
        scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        _runtime: &mut LegacyRuntime,
        trigger: PendingTrigger,
        active: &mut JoinSet<CompletedRun>,
    ) -> Result<bool, SchedulerError> {
        try_spawn_legacy_trigger(scheduler, &self.guard, job, active, trigger).await
    }

    async fn try_reclaim_inflight(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        _job: &Job<D>,
        _runtime: &mut LegacyRuntime,
        _active: &mut JoinSet<CompletedRun>,
    ) -> Result<bool, SchedulerError> {
        Ok(false)
    }

    async fn handle_due_trigger(
        &self,
        scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        runtime: &mut LegacyRuntime,
        candidate_state: JobState,
        _trigger: PendingTrigger,
        overlap_action: OverlapAction,
        active: &mut JoinSet<CompletedRun>,
    ) -> Result<bool, SchedulerError> {
        runtime.state = candidate_state;
        scheduler
            .persist_state_to_legacy(self.store.as_ref(), &runtime.state)
            .await?;

        match overlap_action {
            OverlapAction::Spawn(trigger) => {
                scheduler.emit(trigger_emitted(
                    &job.job_id,
                    trigger.scheduled_at,
                    trigger.catch_up,
                    trigger.trigger_count,
                ));
                try_spawn_legacy_trigger(scheduler, &self.guard, job, active, trigger).await
            }
            OverlapAction::QueueUpdated | OverlapAction::Dropped => Ok(false),
        }
    }

    async fn apply_completed(
        &self,
        scheduler: &Scheduler<S, G, C>,
        runtime: &mut LegacyRuntime,
        history: &mut VecDeque<RunRecord>,
        completed: CompletedRun,
    ) -> Result<(), SchedulerError> {
        let (record, trigger_count) = apply_completed_run(
            &mut runtime.state,
            history,
            scheduler.config.history_limit,
            completed,
        );
        scheduler
            .persist_state_to_legacy(self.store.as_ref(), &runtime.state)
            .await?;
        scheduler.emit(run_completed(&runtime.state.job_id, &record, trigger_count));
        Ok(())
    }

    async fn delete_terminal_state(
        &self,
        scheduler: &Scheduler<S, G, C>,
        job: &Job<D>,
        _runtime: &mut LegacyRuntime,
    ) -> Result<(), SchedulerError> {
        scheduler
            .delete_state_from_legacy(self.store.as_ref(), &job.job_id)
            .await
    }

    async fn pause(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        _job: &Job<D>,
        runtime: &mut LegacyRuntime,
    ) -> Result<bool, SchedulerError> {
        let changed = !runtime.paused;
        runtime.paused = true;
        Ok(changed)
    }

    async fn resume(
        &self,
        _scheduler: &Scheduler<S, G, C>,
        _job: &Job<D>,
        runtime: &mut LegacyRuntime,
    ) -> Result<bool, SchedulerError> {
        let changed = runtime.paused;
        runtime.paused = false;
        Ok(changed)
    }
}

async fn load_or_initialize_legacy_state<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    store: &S,
    job: &Job<D>,
) -> Result<(JobState, bool), SchedulerError>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    match scheduler.load_state_from_legacy(store, &job.job_id).await? {
        Some(state) => restore_legacy_state(scheduler, store, job, state).await,
        None => {
            let state = initial_runtime_state(scheduler, job)?;
            emit_state_loaded(scheduler, &job.job_id, &state, StateLoadSource::New);
            Ok((state, true))
        }
    }
}

async fn restore_legacy_state<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    store: &S,
    job: &Job<D>,
    mut state: JobState,
) -> Result<(JobState, bool), SchedulerError>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    if scheduler.should_repair_interval_state(job, &state) {
        let previous_next_run_at = state.next_run_at;
        state.next_run_at =
            crate::scheduler::trigger_math::initial_next_run_at(job, scheduler.config.timezone)?;
        scheduler.persist_state_to_legacy(store, &state).await?;
        emit_state_repaired(scheduler, &job.job_id, &state, previous_next_run_at);
        emit_state_loaded(scheduler, &job.job_id, &state, StateLoadSource::Repaired);
    } else {
        emit_state_loaded(scheduler, &job.job_id, &state, StateLoadSource::Restored);
    }

    Ok((state, false))
}

async fn try_spawn_legacy_trigger<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    guard: &Arc<G>,
    job: &Job<D>,
    active: &mut JoinSet<CompletedRun>,
    trigger: PendingTrigger,
) -> Result<bool, SchedulerError>
where
    S: StateStore + Send + Sync + 'static,
    G: ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    let slot = match job.guard_scope {
        ExecutionGuardScope::Occurrence => ExecutionSlot::for_occurrence(
            job.job_id.clone(),
            job.execution_resource_id.clone(),
            trigger.scheduled_at,
        ),
        ExecutionGuardScope::Resource => {
            ExecutionSlot::for_resource(job.job_id.clone(), job.execution_resource_id.clone())
        }
    };
    let acquired = guard.acquire(slot).await.map_err(|error| {
        let kind = G::classify_error(&error);
        SchedulerError::execution_guard(error, kind)
    })?;

    let ExecutionGuardAcquire::Acquired(lease) = acquired else {
        scheduler.emit(SchedulerEvent::ExecutionGuardContended {
            job_id: job.job_id.clone(),
            resource_id: job.execution_resource_id.clone(),
            scope: job.guard_scope,
            scheduled_at: Some(trigger.scheduled_at),
            catch_up: trigger.catch_up,
            trigger_count: trigger.trigger_count,
        });
        return Ok(false);
    };

    scheduler.emit(execution_guard_acquired(
        &lease,
        trigger.catch_up,
        trigger.trigger_count,
    ));

    spawn_legacy_trigger(
        active,
        job.task.clone(),
        job.deps.clone(),
        job.job_id.clone(),
        scheduler.config.timezone,
        trigger,
        guard.clone(),
        scheduler.observer.clone(),
        scheduler.control.clone(),
        lease,
    );
    Ok(true)
}
