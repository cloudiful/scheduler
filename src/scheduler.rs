use crate::error::SchedulerError;
use crate::model::{
    Job, JobState, RunContext, RunRecord, RunStatus, Schedule, SchedulerConfig, SchedulerReport,
    push_history,
};
use crate::store::StateStore;
use chrono::{DateTime, TimeDelta, Utc};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
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

    pub async fn run(&self, job: Job) -> Result<SchedulerReport, SchedulerError> {
        let job = self.normalize_job(job)?;
        let mut state = match self
            .store
            .load(&job.job_id)
            .await
            .map_err(SchedulerError::Store)?
        {
            Some(state) => state,
            None => JobState::new(job.job_id.clone(), self.initial_next_run_at(&job)),
        };
        let mut history = VecDeque::new();
        let mut active = JoinSet::new();
        let mut active_count = 0usize;
        let mut queued_trigger = None;
        let _ = self.control.send(ControlSignal::Running);
        let mut control_rx = self.control.subscribe();
        self.store
            .save(&state)
            .await
            .map_err(SchedulerError::Store)?;

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
                } else if let Some(trigger) = self.next_trigger(&job, &mut state, now)? {
                    self.store
                        .save(&state)
                        .await
                        .map_err(SchedulerError::Store)?;
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
                        let completed = result.map_err(|error| SchedulerError::TaskJoin(error.to_string()))?;
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
                        self.store
                            .save(&state)
                            .await
                            .map_err(SchedulerError::Store)?;
                        push_history(&mut history, completed.record, self.config.history_limit);
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
            let completed = result.map_err(|error| SchedulerError::TaskJoin(error.to_string()))?;
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
            self.store
                .save(&state)
                .await
                .map_err(SchedulerError::Store)?;
            push_history(&mut history, completed.record, self.config.history_limit);
        }

        Ok(SchedulerReport {
            job_id: job.job_id.clone(),
            state,
            history: history.into_iter().collect(),
        })
    }

    fn normalize_job(&self, mut job: Job) -> Result<Job, SchedulerError> {
        match &mut job.schedule {
            Schedule::Interval(every) => {
                if every.is_zero() {
                    return Err(SchedulerError::InvalidJob(
                        "interval schedule must be greater than zero".to_string(),
                    ));
                }
            }
            Schedule::AtTimes(times) => {
                times.sort_unstable();
            }
        }

        Ok(job)
    }

    fn initial_next_run_at(&self, job: &Job) -> Option<DateTime<Utc>> {
        if matches!(job.max_runs, Some(0)) {
            return None;
        }

        match &job.schedule {
            Schedule::Interval(every) => {
                duration_to_delta(*every).and_then(|delta| Utc::now().checked_add_signed(delta))
            }
            Schedule::AtTimes(times) => times.first().map(|value| value.with_timezone(&Utc)),
        }
    }

    fn next_trigger(
        &self,
        job: &Job,
        state: &mut JobState,
        now: DateTime<Utc>,
    ) -> Result<Option<PendingTrigger>, SchedulerError> {
        let Some(next_run_at) = state.next_run_at else {
            return Ok(None);
        };

        if next_run_at > now {
            return Ok(None);
        }

        let due_times = self.collect_due_times(job, state, now)?;
        if due_times.is_empty() {
            return Ok(None);
        }

        let first_due = due_times[0];
        if due_times.len() == 1 && !is_missed(first_due, now) {
            self.advance_state_to(job, state, first_due)?;
            return Ok(Some(PendingTrigger {
                scheduled_at: first_due,
                catch_up: false,
            }));
        }

        match job.missed_run_policy {
            crate::MissedRunPolicy::Skip => {
                self.advance_state_for(job, state, &due_times)?;
                Ok(None)
            }
            crate::MissedRunPolicy::CatchUpOnce => {
                let last_due = *due_times.last().expect("due_times is not empty");
                self.advance_state_for(job, state, &due_times)?;
                Ok(Some(PendingTrigger {
                    scheduled_at: last_due,
                    catch_up: due_times.len() > 1 || is_missed(last_due, now),
                }))
            }
            crate::MissedRunPolicy::ReplayAll => {
                self.advance_state_to(job, state, first_due)?;
                Ok(Some(PendingTrigger {
                    scheduled_at: first_due,
                    catch_up: due_times.len() > 1 || is_missed(first_due, now),
                }))
            }
        }
    }

    fn collect_due_times(
        &self,
        job: &Job,
        state: &JobState,
        now: DateTime<Utc>,
    ) -> Result<Vec<DateTime<Utc>>, SchedulerError> {
        let mut due_times = Vec::new();
        let mut trigger_count = state.trigger_count;
        let mut next_run_at = state.next_run_at;

        while let Some(value) = next_run_at {
            if value > now {
                break;
            }

            due_times.push(value);
            trigger_count += 1;
            next_run_at = self.compute_next_after(job, value, trigger_count)?;
        }

        Ok(due_times)
    }

    fn advance_state_to(
        &self,
        job: &Job,
        state: &mut JobState,
        scheduled_at: DateTime<Utc>,
    ) -> Result<(), SchedulerError> {
        state.trigger_count += 1;
        state.next_run_at = self.compute_next_after(job, scheduled_at, state.trigger_count)?;
        Ok(())
    }

    fn advance_state_for(
        &self,
        job: &Job,
        state: &mut JobState,
        due_times: &[DateTime<Utc>],
    ) -> Result<(), SchedulerError> {
        for scheduled_at in due_times {
            self.advance_state_to(job, state, *scheduled_at)?;
        }

        Ok(())
    }

    fn compute_next_after(
        &self,
        job: &Job,
        scheduled_at: DateTime<Utc>,
        trigger_count: u32,
    ) -> Result<Option<DateTime<Utc>>, SchedulerError> {
        if let Some(max_runs) = job.max_runs
            && trigger_count >= max_runs
        {
            return Ok(None);
        }

        match &job.schedule {
            Schedule::Interval(every) => {
                let delta = duration_to_delta(*every).ok_or_else(|| {
                    SchedulerError::InvalidJob(
                        "interval schedule is too large to represent".to_string(),
                    )
                })?;
                Ok(scheduled_at.checked_add_signed(delta))
            }
            Schedule::AtTimes(times) => Ok(times
                .get(trigger_count as usize)
                .map(|value| value.with_timezone(&Utc))),
        }
    }

    fn spawn_trigger(
        &self,
        job: &Job,
        active: &mut JoinSet<CompletedRun>,
        trigger: PendingTrigger,
    ) {
        let task = job.task.clone();
        let timezone = self.config.timezone;
        let job_id = job.job_id.clone();
        active.spawn(async move {
            let started_at = Utc::now();
            let result = task(RunContext {
                job_id,
                scheduled_at: trigger.scheduled_at,
                catch_up: trigger.catch_up,
                timezone,
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

    async fn sleep_until_next(&self, next_run_at: Option<DateTime<Utc>>) {
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
struct PendingTrigger {
    scheduled_at: DateTime<Utc>,
    catch_up: bool,
}

#[derive(Debug)]
struct CompletedRun {
    record: RunRecord,
}

fn duration_to_delta(duration: Duration) -> Option<TimeDelta> {
    TimeDelta::from_std(duration).ok()
}

fn is_missed(scheduled_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    let tolerance = TimeDelta::milliseconds(25);
    scheduled_at
        .checked_add_signed(tolerance)
        .map(|adjusted| adjusted < now)
        .unwrap_or(false)
}

fn next_run_is_in_future(next_run_at: Option<DateTime<Utc>>) -> bool {
    next_run_at.map(|value| value > Utc::now()).unwrap_or(false)
}
