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
//! - [`Job::with_max_runs`] applies to both schedule kinds; `0` exits without
//!   running.
//! - [`SchedulerConfig::timezone`] is forwarded through [`RunContext`] and does
//!   not rewrite absolute [`Schedule::AtTimes`] values.
//! - Restarts resume by `job_id` from the saved [`JobState::next_run_at`].
//! - Dependency injection here means passing an explicit `deps` value when the
//!   job is constructed. The scheduler does not auto-resolve parameters.
//!
//! ```rust
//! use std::time::Duration;
//!
//! use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig};
//!
//! let runtime = tokio::runtime::Runtime::new().unwrap();
//! runtime.block_on(async {
//!     let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
//!     let job = Job::new(
//!         "doc-simple",
//!         Schedule::Interval(Duration::from_millis(1)),
//!         || async { Ok(()) },
//!     )
//!     .with_max_runs(1);
//!
//!     let report = scheduler.run(job).await.unwrap();
//!     assert_eq!(report.history.len(), 1);
//! });
//! ```

mod error;
mod model;
mod scheduler;
mod store;

pub use error::SchedulerError;
pub use model::{
    Job, JobFuture, JobResult, JobState, MissedRunPolicy, OverlapPolicy, RunContext, RunRecord,
    RunStatus, Schedule, SchedulerConfig, SchedulerReport, TaskContext, TaskHandler,
};
pub use scheduler::{Scheduler, SchedulerHandle};
pub use store::{InMemoryStateStore, StateStore};
