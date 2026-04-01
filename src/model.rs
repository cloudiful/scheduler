use chrono::{DateTime, Utc};
use chrono_tz::{Asia::Shanghai, Tz};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub type JobResult = Result<(), String>;
pub type JobFuture = Pin<Box<dyn Future<Output = JobResult> + Send>>;
pub type TaskHandler = Arc<dyn Fn(RunContext) -> JobFuture + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    Interval(Duration),
    AtTimes(Vec<DateTime<Tz>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MissedRunPolicy {
    Skip,
    #[default]
    CatchUpOnce,
    ReplayAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverlapPolicy {
    #[default]
    Forbid,
    QueueOne,
    AllowParallel,
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub timezone: Tz,
    pub history_limit: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            timezone: Shanghai,
            history_limit: 32,
        }
    }
}

#[derive(Clone)]
pub struct Job {
    pub job_id: String,
    pub schedule: Schedule,
    pub max_runs: Option<u32>,
    pub missed_run_policy: MissedRunPolicy,
    pub overlap_policy: OverlapPolicy,
    pub(crate) task: TaskHandler,
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("job_id", &self.job_id)
            .field("schedule", &self.schedule)
            .field("max_runs", &self.max_runs)
            .field("missed_run_policy", &self.missed_run_policy)
            .field("overlap_policy", &self.overlap_policy)
            .finish_non_exhaustive()
    }
}

impl Job {
    pub fn new<F, Fut>(job_id: impl Into<String>, schedule: Schedule, task: F) -> Self
    where
        F: Fn(RunContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JobResult> + Send + 'static,
    {
        Self {
            job_id: job_id.into(),
            schedule,
            max_runs: None,
            missed_run_policy: MissedRunPolicy::CatchUpOnce,
            overlap_policy: OverlapPolicy::Forbid,
            task: Arc::new(move |context| Box::pin(task(context))),
        }
    }

    pub fn with_max_runs(mut self, max_runs: u32) -> Self {
        self.max_runs = Some(max_runs);
        self
    }

    pub fn with_missed_run_policy(mut self, policy: MissedRunPolicy) -> Self {
        self.missed_run_policy = policy;
        self
    }

    pub fn with_overlap_policy(mut self, policy: OverlapPolicy) -> Self {
        self.overlap_policy = policy;
        self
    }
}

#[derive(Debug, Clone)]
pub struct RunContext {
    pub job_id: String,
    pub scheduled_at: DateTime<Utc>,
    pub catch_up: bool,
    pub timezone: Tz,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    pub scheduled_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub catch_up: bool,
    pub status: RunStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobState {
    pub job_id: String,
    pub trigger_count: u32,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

impl JobState {
    pub fn new(job_id: impl Into<String>, next_run_at: Option<DateTime<Utc>>) -> Self {
        Self {
            job_id: job_id.into(),
            trigger_count: 0,
            last_run_at: None,
            last_success_at: None,
            next_run_at,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerReport {
    pub job_id: String,
    pub state: JobState,
    pub history: Vec<RunRecord>,
}

pub(crate) fn push_history(
    history: &mut VecDeque<RunRecord>,
    record: RunRecord,
    history_limit: usize,
) {
    if history_limit == 0 {
        return;
    }

    history.push_back(record);
    while history.len() > history_limit {
        history.pop_front();
    }
}
