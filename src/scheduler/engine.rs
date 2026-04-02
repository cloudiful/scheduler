use crate::error::SchedulerError;
use crate::model::{
    Job, JobState, RunContext, RunRecord, RunStatus, SchedulerConfig, SchedulerReport, TaskContext,
    push_history,
};
use crate::scheduler::trigger::{
    PendingTrigger, initial_next_run_at, next_run_is_in_future, next_trigger,
};
use crate::store::StateStore;
use chrono::Utc;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlSignal {
    Running,
    Cancel,
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct SchedulerHandle {
    control: watch::Sender<ControlSignal>,
}

impl SchedulerHandle {
    pub fn cancel(&self) {
        let _ = self.control.send(ControlSignal::Cancel);
    }

    pub fn shutdown(&self) {
        let _ = self.control.send(ControlSignal::Shutdown);
    }
}

#[derive(Debug)]
pub struct Scheduler<S>
where
    S: StateStore,
{
    config: SchedulerConfig,
    store: Arc<S>,
    control: watch::Sender<ControlSignal>,
}

impl<S> Scheduler<S>
where
    S: StateStore + Send + Sync + 'static,
{
    pub fn new(config: SchedulerConfig, store: S) -> Self {
        let (control, _) = watch::channel(ControlSignal::Running);
        Self {
            config,
            store: Arc::new(store),
            control,
        }
    }

    pub fn handle(&self) -> SchedulerHandle {
        SchedulerHandle {
            control: self.control.clone(),
        }
    }

    pub async fn run<D>(&self, job: Job<D>) -> Result<SchedulerReport, SchedulerError>
    where
        D: Send + Sync + 'static,
    {
        let job = self.normalize_job(job)?;
        let mut state = self.load_or_initialize_state(&job).await?;
        let mut history = VecDeque::new();
        let mut active = JoinSet::new();
        let mut active_count = 0usize;
        let mut queued_trigger = None;
        let _ = self.control.send(ControlSignal::Running);
        let mut control_rx = self.control.subscribe();
        self.persist_state(&state).await?;

        loop {
            if matches!(
                *control_rx.borrow(),
                ControlSignal::Cancel | ControlSignal::Shutdown
            ) && active_count == 0
            {
                break;
            }

            if matches!(*control_rx.borrow(), ControlSignal::Running) {
                if active_count == 0
                    && let Some(trigger) = queued_trigger.take()
                {
                    self.spawn_trigger(&job, &mut active, trigger);
                    active_count += 1;
                    continue;
                }

                let now = Utc::now();
                if active_count > 0
                    && matches!(job.missed_run_policy, crate::MissedRunPolicy::ReplayAll)
                    && !matches!(job.overlap_policy, crate::OverlapPolicy::AllowParallel)
                {
                    // ReplayAll preserves every missed occurrence; serialize it instead of
                    // letting overlap control drop overdue triggers while one run is active.
                } else if let Some(trigger) = next_trigger(&job, &mut state, now)? {
                    self.persist_state(&state).await?;
                    match job.overlap_policy {
                        crate::OverlapPolicy::AllowParallel => {
                            self.spawn_trigger(&job, &mut active, trigger);
                            active_count += 1;
                            continue;
                        }
                        crate::OverlapPolicy::Forbid => {
                            if active_count == 0 {
                                self.spawn_trigger(&job, &mut active, trigger);
                                active_count += 1;
                            }
                            continue;
                        }
                        crate::OverlapPolicy::QueueOne => {
                            if active_count == 0 {
                                self.spawn_trigger(&job, &mut active, trigger);
                                active_count += 1;
                            } else if queued_trigger.is_none() {
                                queued_trigger = Some(trigger);
                            }
                            continue;
                        }
                    }
                }
            }

            if state.next_run_at.is_none() && active_count == 0 && queued_trigger.is_none() {
                break;
            }

            tokio::select! {
                maybe_result = active.join_next(), if active_count > 0 => {
                    if let Some(result) = maybe_result {
                        active_count -= 1;
                        let completed = result.map_err(|error| SchedulerError::task_join(error.to_string()))?;
                        self.apply_completed_run(&mut state, &mut history, completed).await?;
                    }
                }
                changed = control_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                _ = self.sleep_until_next(state.next_run_at), if matches!(*control_rx.borrow(), ControlSignal::Running) && queued_trigger.is_none() && next_run_is_in_future(state.next_run_at) => {}
            }
        }

        while let Some(result) = active.join_next().await {
            let completed = result.map_err(|error| SchedulerError::task_join(error.to_string()))?;
            self.apply_completed_run(&mut state, &mut history, completed)
                .await?;
        }

        Ok(SchedulerReport {
            job_id: job.job_id.clone(),
            state,
            history: history.into_iter().collect(),
        })
    }

    async fn load_or_initialize_state<D>(&self, job: &Job<D>) -> Result<JobState, SchedulerError>
    where
        D: Send + Sync + 'static,
    {
        match self
            .store
            .load(&job.job_id)
            .await
            .map_err(SchedulerError::store)?
        {
            Some(state) => Ok(state),
            None => Ok(JobState::new(job.job_id.clone(), initial_next_run_at(job)?)),
        }
    }

    async fn persist_state(&self, state: &JobState) -> Result<(), SchedulerError> {
        self.store.save(state).await.map_err(SchedulerError::store)
    }

    async fn apply_completed_run(
        &self,
        state: &mut JobState,
        history: &mut VecDeque<RunRecord>,
        completed: CompletedRun,
    ) -> Result<(), SchedulerError> {
        state.last_run_at = Some(completed.record.started_at);
        match completed.record.status {
            RunStatus::Success => {
                state.last_success_at = Some(completed.record.finished_at);
                state.last_error = None;
            }
            RunStatus::Failed => {
                state.last_error = completed.record.error.clone();
            }
        }

        self.persist_state(state).await?;
        push_history(history, completed.record, self.config.history_limit);
        Ok(())
    }

    fn normalize_job<D>(&self, mut job: Job<D>) -> Result<Job<D>, SchedulerError> {
        match &mut job.schedule {
            crate::Schedule::Interval(every) => {
                if every.is_zero() {
                    return Err(SchedulerError::invalid_job(
                        "interval schedule must be greater than zero",
                    ));
                }
            }
            crate::Schedule::AtTimes(times) => {
                times.sort_unstable();
            }
        }

        Ok(job)
    }

    fn spawn_trigger<D>(
        &self,
        job: &Job<D>,
        active: &mut JoinSet<CompletedRun>,
        trigger: PendingTrigger,
    ) where
        D: Send + Sync + 'static,
    {
        let task = job.task.clone();
        let deps = job.deps.clone();
        let timezone = self.config.timezone;
        let job_id = job.job_id.clone();
        active.spawn(async move {
            let started_at = Utc::now();
            let result = task(TaskContext {
                run: RunContext {
                    job_id,
                    scheduled_at: trigger.scheduled_at,
                    catch_up: trigger.catch_up,
                    timezone,
                },
                deps,
            })
            .await;
            let finished_at = Utc::now();

            let (status, error) = match result {
                Ok(()) => (RunStatus::Success, None),
                Err(message) => (RunStatus::Failed, Some(message)),
            };

            CompletedRun {
                record: RunRecord {
                    scheduled_at: trigger.scheduled_at,
                    started_at,
                    finished_at,
                    catch_up: trigger.catch_up,
                    status,
                    error,
                },
            }
        });
    }

    async fn sleep_until_next(&self, next_run_at: Option<chrono::DateTime<Utc>>) {
        let Some(next_run_at) = next_run_at else {
            return;
        };

        let now = Utc::now();
        if let Ok(duration) = (next_run_at - now).to_std() {
            tokio::time::sleep(duration).await;
        }
    }
}

#[derive(Debug)]
struct CompletedRun {
    record: RunRecord,
}
