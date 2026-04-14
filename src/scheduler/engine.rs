use crate::error::SchedulerError;
use crate::model::{
    Job, JobState, RunRecord, SchedulerConfig, SchedulerReport, TerminalStatePolicy,
};
use crate::observer::{
    LogObserver, NoopObserver, SchedulerEvent, SchedulerObserver, SchedulerStopReason,
    StateLoadSource,
};
use crate::scheduler::control::{ControlSignal, SchedulerHandle};
use crate::scheduler::execution::{CompletedRun, spawn_trigger};
use crate::scheduler::overlap::{OverlapAction, dispatch_trigger, take_queued_if_idle};
use crate::scheduler::trigger::{PendingTrigger, TriggerDecision, next_trigger};
use crate::scheduler::trigger_math::{initial_next_run_at, next_run_is_in_future};
use crate::store::{StateStore, StoreEvent};
use chrono::Utc;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

pub struct Scheduler<S>
where
    S: StateStore,
{
    config: SchedulerConfig,
    store: Arc<S>,
    observer: Arc<dyn SchedulerObserver>,
    control: watch::Sender<ControlSignal>,
}

impl<S> Scheduler<S>
where
    S: StateStore + Send + Sync + 'static,
{
    pub fn new(config: SchedulerConfig, store: S) -> Self {
        Self::with_observer(config, store, NoopObserver)
    }

    pub fn with_log_observer(config: SchedulerConfig, store: S) -> Self {
        Self::with_observer(config, store, LogObserver)
    }

    pub fn with_observer<O>(config: SchedulerConfig, store: S, observer: O) -> Self
    where
        O: SchedulerObserver,
    {
        let (control, _) = watch::channel(ControlSignal::Running);
        Self {
            config,
            store: Arc::new(store),
            observer: Arc::new(observer),
            control,
        }
    }

    pub fn handle(&self) -> SchedulerHandle {
        SchedulerHandle::new(self.control.clone())
    }

    pub async fn run<D>(&self, job: Job<D>) -> Result<SchedulerReport, SchedulerError>
    where
        D: Send + Sync + 'static,
    {
        let job = self.normalize_job(job)?;
        let (mut state, state_is_new) = self.load_or_initialize_state(&job).await?;
        let mut history = VecDeque::new();
        let mut active = JoinSet::new();
        let mut active_count = 0usize;
        let mut queued_trigger = None;
        let _ = self.control.send(ControlSignal::Running);
        let mut control_rx = self.control.subscribe();
        if state_is_new {
            self.persist_state(&state).await?;
        }

        loop {
            if matches!(
                *control_rx.borrow(),
                ControlSignal::Cancel | ControlSignal::Shutdown
            ) && active_count == 0
            {
                self.emit(SchedulerEvent::SchedulerStopped {
                    job_id: job.job_id.clone(),
                    trigger_count: state.trigger_count,
                    reason: match *control_rx.borrow() {
                        ControlSignal::Cancel => SchedulerStopReason::Cancelled,
                        ControlSignal::Shutdown => SchedulerStopReason::Shutdown,
                        ControlSignal::Running => SchedulerStopReason::ChannelClosed,
                    },
                });
                break;
            }

            if matches!(*control_rx.borrow(), ControlSignal::Running) {
                if let Some(trigger) = take_queued_if_idle(active_count, &mut queued_trigger) {
                    self.spawn_trigger(&job, &mut active, trigger);
                    active_count += 1;
                    continue;
                }

                let now = Utc::now();
                if self.should_wait_for_active_replay(&job, active_count) {
                    // ReplayAll preserves every missed occurrence; serialize it instead of
                    // letting overlap control drop overdue triggers while one run is active.
                } else {
                    match next_trigger(&job, &mut state, now, self.config.timezone)? {
                        TriggerDecision::Idle => {}
                        TriggerDecision::StateAdvanced => {
                            self.persist_state(&state).await?;
                        }
                        TriggerDecision::Trigger(trigger) => {
                            self.persist_state(&state).await?;
                            self.emit(SchedulerEvent::TriggerEmitted {
                                job_id: job.job_id.clone(),
                                scheduled_at: trigger.scheduled_at,
                                catch_up: trigger.catch_up,
                                trigger_count: state.trigger_count,
                            });
                            match dispatch_trigger(
                                job.overlap_policy,
                                active_count,
                                &mut queued_trigger,
                                trigger,
                            ) {
                                OverlapAction::Spawn(trigger) => {
                                    self.spawn_trigger(&job, &mut active, trigger);
                                    active_count += 1;
                                    continue;
                                }
                                OverlapAction::QueueUpdated | OverlapAction::Dropped => {
                                    continue;
                                }
                            }
                        }
                    }
                }
            }

            if state.next_run_at.is_none() && active_count == 0 && queued_trigger.is_none() {
                if matches!(
                    self.config.terminal_state_policy,
                    TerminalStatePolicy::Delete
                ) {
                    self.delete_state(&job.job_id).await?;
                    self.emit(SchedulerEvent::TerminalStateDeleted {
                        job_id: job.job_id.clone(),
                        trigger_count: state.trigger_count,
                    });
                }
                self.emit(SchedulerEvent::SchedulerStopped {
                    job_id: job.job_id.clone(),
                    trigger_count: state.trigger_count,
                    reason: SchedulerStopReason::Terminal,
                });
                break;
            }

            tokio::select! {
                maybe_result = active.join_next(), if active_count > 0 => {
                    if let Some(result) = maybe_result {
                        active_count -= 1;
                        let completed = result.map_err(SchedulerError::task_join)?;
                        self.apply_completed_run(&mut state, &mut history, completed).await?;
                    }
                }
                changed = control_rx.changed() => {
                    if changed.is_err() {
                        self.emit(SchedulerEvent::SchedulerStopped {
                            job_id: job.job_id.clone(),
                            trigger_count: state.trigger_count,
                            reason: SchedulerStopReason::ChannelClosed,
                        });
                        break;
                    }
                }
                _ = self.sleep_until_next(state.next_run_at), if matches!(*control_rx.borrow(), ControlSignal::Running) && queued_trigger.is_none() && next_run_is_in_future(state.next_run_at) => {}
            }
        }

        while let Some(result) = active.join_next().await {
            let completed = result.map_err(SchedulerError::task_join)?;
            self.apply_completed_run(&mut state, &mut history, completed)
                .await?;
        }

        Ok(SchedulerReport {
            job_id: job.job_id.clone(),
            state,
            history: history.into_iter().collect(),
        })
    }

    async fn load_or_initialize_state<D>(
        &self,
        job: &Job<D>,
    ) -> Result<(JobState, bool), SchedulerError>
    where
        D: Send + Sync + 'static,
    {
        match self.load_state(&job.job_id).await? {
            Some(state) => self.restore_persisted_state(job, state).await,
            None => {
                let state = JobState::new(
                    job.job_id.clone(),
                    initial_next_run_at(job, self.config.timezone)?,
                );
                self.emit(SchedulerEvent::StateLoaded {
                    job_id: job.job_id.clone(),
                    trigger_count: state.trigger_count,
                    next_run_at: state.next_run_at,
                    source: StateLoadSource::New,
                });
                Ok((state, true))
            }
        }
    }

    async fn restore_persisted_state<D>(
        &self,
        job: &Job<D>,
        mut state: JobState,
    ) -> Result<(JobState, bool), SchedulerError>
    where
        D: Send + Sync + 'static,
    {
        if self.should_repair_interval_state(job, &state) {
            let previous_next_run_at = state.next_run_at;
            let repaired_next_run_at = initial_next_run_at(job, self.config.timezone)?;
            state.next_run_at = repaired_next_run_at;
            self.persist_state(&state).await?;
            self.emit(SchedulerEvent::StateRepaired {
                job_id: job.job_id.clone(),
                trigger_count: state.trigger_count,
                previous_next_run_at,
                repaired_next_run_at: state.next_run_at,
            });
            self.emit(SchedulerEvent::StateLoaded {
                job_id: job.job_id.clone(),
                trigger_count: state.trigger_count,
                next_run_at: state.next_run_at,
                source: StateLoadSource::Repaired,
            });
        } else {
            self.emit(SchedulerEvent::StateLoaded {
                job_id: job.job_id.clone(),
                trigger_count: state.trigger_count,
                next_run_at: state.next_run_at,
                source: StateLoadSource::Restored,
            });
        }

        Ok((state, false))
    }

    fn should_repair_interval_state<D>(&self, job: &Job<D>, state: &JobState) -> bool
    where
        D: Send + Sync + 'static,
    {
        if state.next_run_at.is_some() {
            return false;
        }

        if !matches!(job.schedule, crate::Schedule::Interval(_)) {
            return false;
        }

        match job.max_runs {
            None => true,
            Some(max_runs) => state.trigger_count < max_runs,
        }
    }

    async fn persist_state(&self, state: &JobState) -> Result<(), SchedulerError> {
        self.store.save(state).await.map_err(|error| {
            let kind = S::classify_error(&error);
            SchedulerError::store(error, kind)
        })?;
        self.emit_store_events(&state.job_id).await
    }

    async fn delete_state(&self, job_id: &str) -> Result<(), SchedulerError> {
        self.store.delete(job_id).await.map_err(|error| {
            let kind = S::classify_error(&error);
            SchedulerError::store(error, kind)
        })?;
        self.emit_store_events(job_id).await
    }

    async fn load_state(&self, job_id: &str) -> Result<Option<JobState>, SchedulerError> {
        let state = self.store.load(job_id).await.map_err(|error| {
            let kind = S::classify_error(&error);
            SchedulerError::store(error, kind)
        })?;
        self.emit_store_events(job_id).await?;
        Ok(state)
    }

    async fn emit_store_events(&self, job_id: &str) -> Result<(), SchedulerError> {
        let events = self.store.drain_events().await.map_err(|error| {
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
            }
        }

        Ok(())
    }

    fn emit(&self, event: SchedulerEvent) {
        self.observer.on_event(&event);
    }

    async fn apply_completed_run(
        &self,
        state: &mut JobState,
        history: &mut VecDeque<RunRecord>,
        completed: CompletedRun,
    ) -> Result<(), SchedulerError> {
        let record = completed.apply_to(state, history, self.config.history_limit);
        self.persist_state(state).await?;
        self.emit(SchedulerEvent::RunCompleted {
            job_id: state.job_id.clone(),
            scheduled_at: record.scheduled_at,
            catch_up: record.catch_up,
            trigger_count: state.trigger_count,
            status: record.status,
            error: record.error,
        });
        Ok(())
    }

    fn should_wait_for_active_replay<D>(&self, job: &Job<D>, active_count: usize) -> bool {
        active_count > 0
            && matches!(job.missed_run_policy, crate::MissedRunPolicy::ReplayAll)
            && !matches!(job.overlap_policy, crate::OverlapPolicy::AllowParallel)
    }

    fn normalize_job<D>(&self, mut job: Job<D>) -> Result<Job<D>, SchedulerError> {
        match &mut job.schedule {
            crate::Schedule::Interval(every) => {
                if every.is_zero() {
                    return Err(SchedulerError::invalid_zero_interval());
                }
            }
            crate::Schedule::AtTimes(times) => {
                times.sort_unstable();
            }
            crate::Schedule::Cron(_) => {}
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
        spawn_trigger(
            active,
            job.task.clone(),
            job.deps.clone(),
            job.job_id.clone(),
            self.config.timezone,
            trigger,
        );
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
