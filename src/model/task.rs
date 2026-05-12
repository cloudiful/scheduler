use crate::execution_guard::ExecutionGuardScope;
use crate::model::{
    JobTimeWindow, MissedRunPolicy, OverlapPolicy, RunSkipReason, Schedule, TimeWindowAlignment,
};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use std::any::type_name;
use std::future::{Future, ready};
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::Arc;

/// The task return type used by scheduled jobs.
pub type JobResult = Result<(), String>;
/// The boxed future returned by a scheduled job.
pub type JobFuture = Pin<Box<dyn Future<Output = JobResult> + Send>>;
pub(crate) type TaskHandler<D> = Arc<dyn Fn(TaskContext<D>) -> JobFuture + Send + Sync>;

#[derive(Clone)]
pub struct Task<D> {
    pub(crate) handler: TaskHandler<D>,
}

impl<D> Task<D>
where
    D: Send + Sync + 'static,
{
    fn from_handler(handler: TaskHandler<D>) -> Self {
        Self { handler }
    }

    /// Create an async task from the full [`TaskContext`].
    pub fn from_async<F, Fut>(task: F) -> Self
    where
        F: Fn(TaskContext<D>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JobResult> + Send + 'static,
    {
        Self::from_handler(wrap_async_handler(Arc::new(task)))
    }

    /// Create a lightweight synchronous task from the full [`TaskContext`].
    pub fn from_sync<F>(task: F) -> Self
    where
        F: Fn(TaskContext<D>) -> JobResult + Send + Sync + 'static,
    {
        Self::from_handler(wrap_sync_handler(Arc::new(task)))
    }

    /// Create a blocking synchronous task from the full [`TaskContext`].
    pub fn from_blocking<F>(task: F) -> Self
    where
        F: Fn(TaskContext<D>) -> JobResult + Send + Sync + 'static,
    {
        Self::from_handler(wrap_blocking_handler(Arc::new(task)))
    }
}

#[derive(Clone)]
pub struct Job<D = ()> {
    pub job_id: String,
    pub execution_resource_id: String,
    pub guard_scope: ExecutionGuardScope,
    pub schedule: Schedule,
    pub time_window: Option<JobTimeWindow>,
    pub time_window_alignment: TimeWindowAlignment,
    pub max_runs: Option<u32>,
    pub missed_run_policy: MissedRunPolicy,
    pub overlap_policy: OverlapPolicy,
    pub(crate) task: TaskHandler<D>,
    pub(crate) deps: Arc<D>,
}

impl Job<()> {
    /// Create a job that uses no injected dependencies.
    pub fn without_deps(job_id: impl Into<String>, schedule: Schedule, task: Task<()>) -> Self {
        Self::from_parts(job_id.into(), schedule, Arc::new(()), task)
    }
}

impl<D> Job<D>
where
    D: Send + Sync + 'static,
{
    /// Create a job from explicit dependencies and a task handler.
    pub fn new(
        job_id: impl Into<String>,
        schedule: Schedule,
        deps: impl Into<Arc<D>>,
        task: Task<D>,
    ) -> Self {
        Self::from_parts(job_id.into(), schedule, deps.into(), task)
    }
}

impl<D> Job<D> {
    fn default_policies() -> (MissedRunPolicy, OverlapPolicy) {
        (MissedRunPolicy::CatchUpOnce, OverlapPolicy::Forbid)
    }

    fn from_parts(job_id: String, schedule: Schedule, deps: Arc<D>, task: Task<D>) -> Self {
        let (missed_run_policy, overlap_policy) = Self::default_policies();
        Self {
            execution_resource_id: job_id.clone(),
            job_id,
            guard_scope: ExecutionGuardScope::Occurrence,
            schedule,
            time_window: None,
            time_window_alignment: TimeWindowAlignment::Disabled,
            max_runs: None,
            missed_run_policy,
            overlap_policy,
            task: task.handler,
            deps,
        }
    }

    /// Limit how many triggers this job can consume before it exits.
    ///
    /// This applies to [`Schedule::Interval`], [`Schedule::StaggeredInterval`],
    /// [`Schedule::GroupedInterval`], [`Schedule::WindowedInterval`],
    /// [`Schedule::AtTimes`], and [`Schedule::Cron`].
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

    pub fn with_time_window(mut self, time_window: JobTimeWindow) -> Self {
        self.time_window = Some(time_window);
        self
    }

    pub fn with_time_window_alignment(mut self) -> Self {
        self.time_window_alignment = TimeWindowAlignment::AlignToNextWindow;
        self
    }

    pub fn with_execution_resource_id(mut self, resource_id: impl Into<String>) -> Self {
        self.execution_resource_id = resource_id.into();
        self
    }

    pub fn with_guard_scope(mut self, scope: ExecutionGuardScope) -> Self {
        self.guard_scope = scope;
        self
    }

    pub(crate) fn skip_reason_at(
        &self,
        now: DateTime<Utc>,
        fallback_timezone: Tz,
    ) -> Option<RunSkipReason> {
        let window = self.time_window.as_ref()?;
        if window.matches(now, fallback_timezone) {
            None
        } else {
            Some(RunSkipReason::OutsideTimeWindow)
        }
    }
}

impl<D> std::fmt::Debug for Job<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("job_id", &self.job_id)
            .field("execution_resource_id", &self.execution_resource_id)
            .field("guard_scope", &self.guard_scope)
            .field("schedule", &self.schedule)
            .field("time_window", &self.time_window)
            .field("time_window_alignment", &self.time_window_alignment)
            .field("max_runs", &self.max_runs)
            .field("missed_run_policy", &self.missed_run_policy)
            .field("overlap_policy", &self.overlap_policy)
            .field("deps", &type_name::<D>())
            .finish_non_exhaustive()
    }
}

fn wrap_async_handler<D, F, Fut>(task: Arc<F>) -> TaskHandler<D>
where
    D: Send + Sync + 'static,
    F: Fn(TaskContext<D>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    Arc::new(move |context| Box::pin((*task)(context)))
}

fn wrap_sync_handler<D, F>(task: Arc<F>) -> TaskHandler<D>
where
    D: Send + Sync + 'static,
    F: Fn(TaskContext<D>) -> JobResult + Send + Sync + 'static,
{
    Arc::new(move |context| Box::pin(ready((*task)(context))))
}

fn wrap_blocking_handler<D, F>(task: Arc<F>) -> TaskHandler<D>
where
    D: Send + Sync + 'static,
    F: Fn(TaskContext<D>) -> JobResult + Send + Sync + 'static,
{
    Arc::new(move |context| {
        let task = task.clone();
        Box::pin(async move { await_blocking(move || (*task)(context)).await })
    })
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
