use chrono::{DateTime, Utc};
use chrono_tz::{Asia::Shanghai, Tz};
use std::any::type_name;
use std::collections::VecDeque;
use std::future::{Future, ready};
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// The task return type used by scheduled jobs.
pub type JobResult = Result<(), String>;
/// The boxed future returned by a scheduled job.
pub type JobFuture = Pin<Box<dyn Future<Output = JobResult> + Send>>;
/// The internal task handler shape used by the scheduler runtime.
pub type TaskHandler<D> = Arc<dyn Fn(TaskContext<D>) -> JobFuture + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// Trigger repeatedly after the given interval.
    Interval(Duration),
    /// Trigger at the listed wall-clock times.
    ///
    /// The list is sorted before execution starts. An empty list is treated as
    /// a no-op schedule and exits without running.
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
    /// The timezone forwarded to each [`RunContext`].
    ///
    /// This does not rewrite [`Schedule::AtTimes`] values, which already carry
    /// their own timezone-aware timestamps.
    pub timezone: Tz,
    /// The maximum number of [`RunRecord`] items kept in memory.
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
pub struct Job<D = ()> {
    pub job_id: String,
    pub schedule: Schedule,
    pub max_runs: Option<u32>,
    pub missed_run_policy: MissedRunPolicy,
    pub overlap_policy: OverlapPolicy,
    pub(crate) task: TaskHandler<D>,
    pub(crate) deps: Arc<D>,
}

impl<D> std::fmt::Debug for Job<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("job_id", &self.job_id)
            .field("schedule", &self.schedule)
            .field("max_runs", &self.max_runs)
            .field("missed_run_policy", &self.missed_run_policy)
            .field("overlap_policy", &self.overlap_policy)
            .field("deps", &type_name::<D>())
            .finish_non_exhaustive()
    }
}

impl Job<()> {
    /// Create an async job with no injected dependencies.
    pub fn new<F, Fut>(job_id: impl Into<String>, schedule: Schedule, task: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JobResult> + Send + 'static,
    {
        Self::from_parts(job_id.into(), schedule, Arc::new(()), async_no_args(task))
    }

    /// Create a lightweight synchronous job with no injected dependencies.
    pub fn new_sync<F>(job_id: impl Into<String>, schedule: Schedule, task: F) -> Self
    where
        F: Fn() -> JobResult + Send + Sync + 'static,
    {
        Self::from_parts(job_id.into(), schedule, Arc::new(()), sync_no_args(task))
    }

    /// Create a blocking synchronous job with no injected dependencies.
    pub fn new_blocking<F>(job_id: impl Into<String>, schedule: Schedule, task: F) -> Self
    where
        F: Fn() -> JobResult + Send + Sync + 'static,
    {
        Self::from_parts(
            job_id.into(),
            schedule,
            Arc::new(()),
            blocking_no_args(task),
        )
    }

    /// Create an async job that consumes [`RunContext`].
    pub fn new_with_run<F, Fut>(job_id: impl Into<String>, schedule: Schedule, task: F) -> Self
    where
        F: Fn(RunContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JobResult> + Send + 'static,
    {
        Self::from_parts(job_id.into(), schedule, Arc::new(()), async_with_run(task))
    }

    /// Create a lightweight synchronous job that consumes [`RunContext`].
    pub fn new_sync_with_run<F>(job_id: impl Into<String>, schedule: Schedule, task: F) -> Self
    where
        F: Fn(RunContext) -> JobResult + Send + Sync + 'static,
    {
        Self::from_parts(job_id.into(), schedule, Arc::new(()), sync_with_run(task))
    }

    /// Create a blocking synchronous job that consumes [`RunContext`].
    pub fn new_blocking_with_run<F>(job_id: impl Into<String>, schedule: Schedule, task: F) -> Self
    where
        F: Fn(RunContext) -> JobResult + Send + Sync + 'static,
    {
        Self::from_parts(
            job_id.into(),
            schedule,
            Arc::new(()),
            blocking_with_run(task),
        )
    }
}

impl<D> Job<D>
where
    D: Send + Sync + 'static,
{
    /// Create an async job with injected dependencies.
    pub fn new_with<F, Fut>(job_id: impl Into<String>, schedule: Schedule, deps: D, task: F) -> Self
    where
        F: Fn(Arc<D>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JobResult> + Send + 'static,
    {
        Self::from_parts(
            job_id.into(),
            schedule,
            Arc::new(deps),
            async_with_deps(task),
        )
    }

    /// Create a lightweight synchronous job with injected dependencies.
    pub fn new_sync_with<F>(job_id: impl Into<String>, schedule: Schedule, deps: D, task: F) -> Self
    where
        F: Fn(Arc<D>) -> JobResult + Send + Sync + 'static,
    {
        Self::from_parts(
            job_id.into(),
            schedule,
            Arc::new(deps),
            sync_with_deps(task),
        )
    }

    /// Create a blocking synchronous job with injected dependencies.
    pub fn new_blocking_with<F>(
        job_id: impl Into<String>,
        schedule: Schedule,
        deps: D,
        task: F,
    ) -> Self
    where
        F: Fn(Arc<D>) -> JobResult + Send + Sync + 'static,
    {
        Self::from_parts(
            job_id.into(),
            schedule,
            Arc::new(deps),
            blocking_with_deps(task),
        )
    }

    /// Create an async job that consumes the full [`TaskContext`].
    pub fn new_with_context<F, Fut>(
        job_id: impl Into<String>,
        schedule: Schedule,
        deps: D,
        task: F,
    ) -> Self
    where
        F: Fn(TaskContext<D>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JobResult> + Send + 'static,
    {
        Self::from_parts(
            job_id.into(),
            schedule,
            Arc::new(deps),
            async_with_context(task),
        )
    }

    /// Create a lightweight synchronous job that consumes the full
    /// [`TaskContext`].
    pub fn new_sync_with_context<F>(
        job_id: impl Into<String>,
        schedule: Schedule,
        deps: D,
        task: F,
    ) -> Self
    where
        F: Fn(TaskContext<D>) -> JobResult + Send + Sync + 'static,
    {
        Self::from_parts(
            job_id.into(),
            schedule,
            Arc::new(deps),
            sync_with_context(task),
        )
    }

    /// Create a blocking synchronous job that consumes the full
    /// [`TaskContext`].
    pub fn new_blocking_with_context<F>(
        job_id: impl Into<String>,
        schedule: Schedule,
        deps: D,
        task: F,
    ) -> Self
    where
        F: Fn(TaskContext<D>) -> JobResult + Send + Sync + 'static,
    {
        Self::from_parts(
            job_id.into(),
            schedule,
            Arc::new(deps),
            blocking_with_context(task),
        )
    }
}

impl<D> Job<D> {
    fn from_parts(job_id: String, schedule: Schedule, deps: Arc<D>, task: TaskHandler<D>) -> Self {
        Self {
            job_id,
            schedule,
            max_runs: None,
            missed_run_policy: MissedRunPolicy::CatchUpOnce,
            overlap_policy: OverlapPolicy::Forbid,
            task,
            deps,
        }
    }

    /// Limit how many triggers this job can consume before it exits.
    ///
    /// This applies to both [`Schedule::Interval`] and [`Schedule::AtTimes`].
    /// A value of `0` makes the job exit immediately without running.
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

fn async_no_args<F, Fut>(task: F) -> TaskHandler<()>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |_| Box::pin((*task)()))
}

fn sync_no_args<F>(task: F) -> TaskHandler<()>
where
    F: Fn() -> JobResult + Send + Sync + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |_| Box::pin(ready((*task)())))
}

fn blocking_no_args<F>(task: F) -> TaskHandler<()>
where
    F: Fn() -> JobResult + Send + Sync + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |_| {
        let task = task.clone();
        Box::pin(async move { await_blocking(move || (*task)()).await })
    })
}

fn async_with_run<F, Fut>(task: F) -> TaskHandler<()>
where
    F: Fn(RunContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |context| Box::pin((*task)(context.run.clone())))
}

fn sync_with_run<F>(task: F) -> TaskHandler<()>
where
    F: Fn(RunContext) -> JobResult + Send + Sync + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |context| Box::pin(ready((*task)(context.run.clone()))))
}

fn blocking_with_run<F>(task: F) -> TaskHandler<()>
where
    F: Fn(RunContext) -> JobResult + Send + Sync + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |context| {
        let task = task.clone();
        let run = context.run.clone();
        Box::pin(async move { await_blocking(move || (*task)(run)).await })
    })
}

fn async_with_deps<D, F, Fut>(task: F) -> TaskHandler<D>
where
    D: Send + Sync + 'static,
    F: Fn(Arc<D>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |context| Box::pin((*task)(context.deps.clone())))
}

fn sync_with_deps<D, F>(task: F) -> TaskHandler<D>
where
    D: Send + Sync + 'static,
    F: Fn(Arc<D>) -> JobResult + Send + Sync + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |context| Box::pin(ready((*task)(context.deps.clone()))))
}

fn blocking_with_deps<D, F>(task: F) -> TaskHandler<D>
where
    D: Send + Sync + 'static,
    F: Fn(Arc<D>) -> JobResult + Send + Sync + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |context| {
        let task = task.clone();
        let deps = context.deps.clone();
        Box::pin(async move { await_blocking(move || (*task)(deps)).await })
    })
}

fn async_with_context<D, F, Fut>(task: F) -> TaskHandler<D>
where
    D: Send + Sync + 'static,
    F: Fn(TaskContext<D>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |context| Box::pin((*task)(context)))
}

fn sync_with_context<D, F>(task: F) -> TaskHandler<D>
where
    D: Send + Sync + 'static,
    F: Fn(TaskContext<D>) -> JobResult + Send + Sync + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |context| Box::pin(ready((*task)(context))))
}

fn blocking_with_context<D, F>(task: F) -> TaskHandler<D>
where
    D: Send + Sync + 'static,
    F: Fn(TaskContext<D>) -> JobResult + Send + Sync + 'static,
{
    let task = Arc::new(task);
    Arc::new(move |context| {
        let task = task.clone();
        Box::pin(async move { await_blocking(move || (*task)(context)).await })
    })
}

async fn await_blocking<F>(task: F) -> JobResult
where
    F: FnOnce() -> JobResult + Send + 'static,
{
    match tokio::task::spawn_blocking(task).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
        Err(error) => panic!("blocking task failed to join: {error}"),
    }
}

#[derive(Debug, Clone)]
pub struct RunContext {
    pub job_id: String,
    pub scheduled_at: DateTime<Utc>,
    pub catch_up: bool,
    /// The scheduler-configured timezone for downstream task logic.
    pub timezone: Tz,
}

#[derive(Clone)]
pub struct TaskContext<D> {
    pub run: RunContext,
    pub deps: Arc<D>,
}

impl<D> std::fmt::Debug for TaskContext<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskContext")
            .field("run", &self.run)
            .field("deps", &type_name::<D>())
            .finish()
    }
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
