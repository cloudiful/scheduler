# Scheduler

`scheduler` is a single-job async scheduling library for background ingestion tasks.

Version `0.2.0` is a breaking refactor. The crate now exposes:

- explicit schedules via `Schedule::Interval` or `Schedule::AtTimes`
- missed-run handling via `MissedRunPolicy`
- overlap control via `OverlapPolicy`
- persistent job state via `StateStore`
- bounded execution history via `SchedulerReport`

The scheduler is responsible only for deciding when to trigger work. Domain-specific recovery, cursor logic, and idempotent refresh commands stay in the caller.

## Add the crate

```toml
[dependencies]
scheduler = { git = "https://github.com/cloudiful/crates-scheduler.git", tag = "v0.2.0" }
chrono = "0.4"
chrono-tz = "0.10"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

## Core concepts

- `Scheduler::new(config, store)` creates the runtime.
- `Job::new(job_id, schedule, task)` defines an async task with no injected dependencies.
- `Job::new_with_run(job_id, schedule, task)` defines a task that receives `RunContext`.
- `Job::new_with(job_id, schedule, deps, task)` defines a task that receives injected `deps`.
- `Job::new_with_context(job_id, schedule, deps, task)` defines a task that receives both `RunContext` and `deps` via `TaskContext`.
- `Job::new_sync*` variants run lightweight synchronous work inline.
- `Job::new_blocking*` variants run blocking synchronous work via `tokio::task::spawn_blocking`.
- `Scheduler::run(job)` runs until the schedule finishes or a control handle requests cancel or shutdown.
- `SchedulerHandle::cancel()` stops while waiting.
- `SchedulerHandle::shutdown()` stops accepting new work and waits for the current run to finish.

Dependency injection in this crate is explicit: you pass a dependency value at job construction time. The scheduler does not auto-resolve arbitrary function parameters.

## Example: async task without dependencies

```rust
use std::time::Duration;

use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig};

#[tokio::main]
async fn main() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new(
        "refresh-cache",
        Schedule::Interval(Duration::from_secs(5)),
        || async move {
            // Call your async command here.
            Ok(())
        },
    )
    .with_max_runs(1);

    let report = scheduler.run(job).await.unwrap();
    println!("history length: {}", report.history.len());
}
```

## Example: task with RunContext

```rust
use chrono::{TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use scheduler::{
    InMemoryStateStore, Job, MissedRunPolicy, OverlapPolicy, RunContext, Schedule, Scheduler,
    SchedulerConfig,
};

#[tokio::main]
async fn main() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new_with_run(
        "refresh-a-shares",
        Schedule::AtTimes(vec![
            Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(5),
            Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(10),
        ]),
        |context: RunContext| async move {
            println!("scheduled for {}", context.scheduled_at);
            // Call your idempotent refresh command here.
            Ok(())
        },
    )
    .with_missed_run_policy(MissedRunPolicy::CatchUpOnce)
    .with_overlap_policy(OverlapPolicy::Forbid);

    let report = scheduler.run(job).await.unwrap();
    println!("final state: {:?}", report.state);
    println!("history length: {}", report.history.len());
}
```

## Example: injected dependencies

Use `deps` to carry any number of business parameters as a struct or tuple.

```rust
use std::sync::Arc;

use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, TaskContext};

#[derive(Debug)]
struct RefreshDeps {
    market: &'static str,
}

#[tokio::main]
async fn main() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new_with_context(
        "refresh-market",
        Schedule::AtTimes(Vec::new()),
        RefreshDeps { market: "A-share" },
        |context: TaskContext<RefreshDeps>| async move {
            let deps: Arc<RefreshDeps> = context.deps.clone();
            println!("market: {}", deps.market);
            println!("scheduled for {}", context.run.scheduled_at);
            Ok(())
        },
    );

    let report = scheduler.run(job).await.unwrap();
    println!("history length: {}", report.history.len());
}
```

## Example: recover state across restarts

Share the same store instance across scheduler instances to resume from `next_run_at`.

```rust
use std::sync::Arc;

use chrono::{TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig};

#[tokio::main]
async fn main() {
    let store = Arc::new(InMemoryStateStore::new());

    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let job = Job::new(
        "resume-me",
        Schedule::AtTimes(vec![Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(30)]),
        || async move { Ok(()) },
    );

    let _ = scheduler.run(job).await;

    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let job = Job::new(
        "resume-me",
        Schedule::AtTimes(vec![Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(30)]),
        || async move { Ok(()) },
    );

    let _ = scheduler.run(job).await;
}
```

## Scheduling semantics

- `Schedule::AtTimes` never executes immediately on startup. The first run waits until the first planned time.
- `Schedule::AtTimes(Vec::new())` is a valid no-op schedule and exits without running.
- `Schedule::Interval` schedules the first run at `now + interval`.
- `max_runs` applies to both interval schedules and explicit `AtTimes` schedules.
- `with_max_runs(0)` exits immediately without running any task.
- `Job::new_sync*` is for lightweight synchronous logic. Use `Job::new_blocking*` for blocking I/O or CPU-heavy synchronous work.
- `MissedRunPolicy::Skip` drops missed occurrences and waits for the next future trigger.
- `MissedRunPolicy::CatchUpOnce` runs one immediate compensating execution for missed occurrences.
- `MissedRunPolicy::ReplayAll` replays each missed occurrence in schedule order.
- `OverlapPolicy::Forbid` skips triggers while a run is active.
- `OverlapPolicy::QueueOne` keeps at most one pending trigger while a run is active.
- `OverlapPolicy::AllowParallel` spawns overlapping runs.
- `SchedulerConfig::timezone` is forwarded to `RunContext` and does not rewrite absolute `AtTimes` timestamps.
- State recovery is keyed by `job_id`; restarting with the same `job_id` resumes from the stored `next_run_at`.
