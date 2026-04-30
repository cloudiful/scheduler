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
//! - [`Schedule::Cron`] evaluates a standard 5-field cron expression in
//!   [`SchedulerConfig::timezone`].
//! - [`Job::with_max_runs`] applies to every schedule kind; `0` exits without
//!   running.
//! - [`SchedulerConfig::timezone`] is forwarded through [`RunContext`], drives
//!   [`Schedule::Cron`] evaluation, and does not rewrite absolute
//!   [`Schedule::AtTimes`] values.
//! - Restarts resume by `job_id` from the saved [`JobState::next_run_at`].
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
#[cfg(feature = "valkey-store")]
mod valkey_store;

pub use coordinated_store::{
    CoordinatedClaim, CoordinatedLeaseConfig, CoordinatedPendingTrigger, CoordinatedRuntimeState,
    CoordinatedStateStore, NoopCoordinatedStateStore,
};
pub use error::{
    ExecutionGuardError, ExecutionGuardErrorKind, InvalidJobError, InvalidJobKind, SchedulerError,
    StoreError, StoreErrorKind, TaskJoinError, TaskJoinErrorKind,
};
pub use execution_guard::{
    ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardRenewal, ExecutionGuardScope,
    ExecutionLease, ExecutionSlot, NoopExecutionGuard,
};
pub use guarded_runner::{GuardedRunResult, GuardedRunner};
pub use model::{
    CronSchedule, Job, JobFuture, JobResult, JobState, MissedRunPolicy, OverlapPolicy, RunContext,
    RunRecord, RunStatus, Schedule, SchedulerConfig, SchedulerReport, Task, TaskContext,
    TerminalStatePolicy,
};
pub use observer::{
    LogObserver, NoopObserver, SchedulerEvent, SchedulerObserver, SchedulerStopReason,
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
#[cfg(feature = "valkey-store")]
pub use valkey_store::ValkeyStateStore;
