use super::engine::Scheduler;
use super::runtime_events::{state_loaded, state_repaired};
use crate::error::SchedulerError;
use crate::model::{Job, JobState};
use crate::observer::StateLoadSource;

pub(super) fn initial_runtime_state<S, G, C, D>(
    scheduler: &Scheduler<S, G, C>,
    job: &Job<D>,
) -> Result<JobState, SchedulerError>
where
    S: crate::StateStore + Send + Sync + 'static,
    G: crate::ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
    D: Send + Sync + 'static,
{
    Ok(JobState::new(
        job.job_id.clone(),
        crate::scheduler::trigger_math::initial_next_run_at(job, scheduler.config.timezone)?,
    ))
}

pub(super) fn emit_state_loaded<S, G, C>(
    scheduler: &Scheduler<S, G, C>,
    job_id: &str,
    state: &JobState,
    source: StateLoadSource,
) where
    S: crate::StateStore + Send + Sync + 'static,
    G: crate::ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
{
    scheduler.emit(state_loaded(job_id, state, source));
}

pub(super) fn emit_state_repaired<S, G, C>(
    scheduler: &Scheduler<S, G, C>,
    job_id: &str,
    state: &JobState,
    previous_next_run_at: Option<chrono::DateTime<chrono::Utc>>,
) where
    S: crate::StateStore + Send + Sync + 'static,
    G: crate::ExecutionGuard + Send + Sync + 'static,
    C: crate::CoordinatedStateStore + Send + Sync + 'static,
{
    scheduler.emit(state_repaired(job_id, state, previous_next_run_at));
}
