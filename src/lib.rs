//! Async scheduling for a single logical job at a time.
//!
//! The scheduler decides when to trigger work and persists the resulting job
//! state through a [`StateStore`]. Domain-specific retry, idempotency, and
//! cursor management remain in the caller.
//!
//! Key semantics:
//!
//! - [`Schedule::AtTimes`] waits until each planned timestamp and treats an
//!   empty list as a no-op schedule.
//! - [`Schedule::Interval`] schedules the first run at `now + interval`.
//! - [`Schedule::StaggeredInterval`] spreads interval jobs by a stable phase
//!   derived from the job id or an explicit seed.
//! - [`Schedule::GroupedInterval`] spreads known group members evenly across
//!   each interval.
//! - [`Schedule::GroupedCron`] spreads known group members evenly inside a
//!   stable post-cron window for each cron anchor.
//! - [`Schedule::WindowedInterval`] selects an interval by local time windows;
//!   `None` intervals disable triggers for the matching period.
//! - [`Schedule::Cron`] evaluates a standard 5-field cron expression in
//!   [`SchedulerConfig::timezone`].
//! - [`JobTimeWindow`] can restrict execution by local weekday and time
//!   segments; outside-window occurrences are skipped with
//!   [`RunSkipReason::OutsideTimeWindow`].
//! - [`Job::with_max_runs`] applies to every schedule kind; `0` exits without
//!   running.
//! - [`SchedulerConfig::timezone`] is forwarded through [`RunContext`], drives
//!   [`Schedule::Cron`] evaluation, and does not rewrite absolute
//!   [`Schedule::AtTimes`] values.
//! - Restarts resume by `job_id` from the saved [`JobState::next_run_at`].
//! - [`SchedulerHandle::pause`] pauses future scheduling without interrupting
//!   the current run. [`SchedulerHandle::resume`] recomputes immediately and
//!   applies the existing missed-run policy.
//! - Pause scope is backend-specific: legacy schedulers pause locally, while
//!   coordinated schedulers persist a shared pause state per `job_id`.
//! - Dependency injection here means passing an explicit `deps` value when the
//!   job is constructed. The scheduler does not auto-resolve parameters.
//!
//! ```rust
//! use std::time::Duration;
//!
//! use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, Task};
//!
//! let runtime = tokio::runtime::Runtime::new().unwrap();
//! runtime.block_on(async {
//!     let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
//!     let job = Job::without_deps(
//!         "doc-simple",
//!         Schedule::Interval(Duration::from_millis(1)),
//!         Task::from_async(|_| async { Ok(()) }),
//!     )
//!     .with_max_runs(1);
//!
//!     let report = scheduler.run(job).await.unwrap();
//!     assert_eq!(report.history.len(), 1);
//! });
//! ```

mod coordinated_store;
mod error;
mod execution_guard;
mod guarded_runner;
mod model;
mod observer;
mod scheduler;
mod store;
#[cfg(feature = "valkey-store")]
mod valkey_coordinated_store;
#[cfg(any(feature = "valkey-guard", feature = "valkey-store"))]
mod valkey_execution_support;
#[cfg(feature = "valkey-guard")]
mod valkey_guard;
#[cfg(any(feature = "valkey-guard", feature = "valkey-store"))]
mod valkey_runtime;
#[cfg(any(feature = "valkey-guard", feature = "valkey-store"))]
mod valkey_scripts;
#[cfg(feature = "valkey-store")]
mod valkey_store;

pub use coordinated_store::{
    CoordinatedClaim, CoordinatedCompletion, CoordinatedLeaseConfig, CoordinatedPendingTrigger,
    CoordinatedRuntimeState, CoordinatedStateStore, NoopCoordinatedStateStore,
};
pub use error::{
    ExecutionGuardError, ExecutionGuardErrorKind, InvalidJobError, InvalidJobKind, SchedulerError,
    StoreError, StoreErrorKind, TaskJoinError, TaskJoinErrorKind,
};
pub use execution_guard::{
    ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardEvent, ExecutionGuardRenewal,
    ExecutionGuardScope, ExecutionLease, ExecutionSlot, NoopExecutionGuard,
};
pub use guarded_runner::{GuardedRunResult, GuardedRunner};
pub use model::{
    CronSchedule, GroupedCronSchedule, GroupedIntervalSchedule, IntervalWindow, Job, JobFuture,
    JobResult, JobState, JobTimeWindow, MissedRunPolicy, OverlapPolicy, RunContext, RunRecord,
    RunSkipReason, RunStatus, Schedule, SchedulerConfig, SchedulerReport,
    StaggeredIntervalSchedule, Task, TaskContext, TerminalStatePolicy, TimeWindowAlignment,
    TimeWindowSegment, TriggerSource, TriggeredTaskContext, WindowedIntervalSchedule,
};
pub use observer::{
    LogObserver, NoopObserver, PauseScope, SchedulerEvent, SchedulerObserver, SchedulerStopReason,
    StateLoadSource,
};
pub use scheduler::{Scheduler, SchedulerHandle};
pub use store::{
    InMemoryStateStore, ResilientStateStore, ResilientStoreError, StateStore, StoreEvent,
    StoreOperation,
};
#[cfg(feature = "valkey-store")]
pub use valkey_coordinated_store::ValkeyCoordinatedStateStore;
#[cfg(feature = "valkey-guard")]
pub use valkey_guard::{ValkeyExecutionGuard, ValkeyLeaseConfig};
#[cfg(any(feature = "valkey-guard", feature = "valkey-store"))]
pub use valkey_runtime::ValkeyRecoveryConfig;
#[cfg(feature = "valkey-store")]
pub use valkey_store::ValkeyStateStore;
