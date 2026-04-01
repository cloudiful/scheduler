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
- `Job::new(job_id, schedule, task)` defines one scheduled task.
- `Scheduler::run(job)` runs until the schedule finishes or a control handle requests cancel or shutdown.
- `SchedulerHandle::cancel()` stops while waiting.
- `SchedulerHandle::shutdown()` stops accepting new work and waits for the current run to finish.

## Example: explicit Shanghai schedule

```rust
use chrono::{TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;
use scheduler::{
    InMemoryStateStore, Job, MissedRunPolicy, OverlapPolicy, Schedule, Scheduler,
    SchedulerConfig,
};

#[tokio::main]
async fn main() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new(
        "refresh-a-shares",
        Schedule::AtTimes(vec![
            Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(5),
            Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(10),
        ]),
        |_context| async move {
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
        |_context| async move { Ok(()) },
    );

    let _ = scheduler.run(job).await;

    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let job = Job::new(
        "resume-me",
        Schedule::AtTimes(vec![Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(30)]),
        |_context| async move { Ok(()) },
    );

    let _ = scheduler.run(job).await;
}
```

## Scheduling semantics

- `Schedule::AtTimes` never executes immediately on startup. The first run waits until the first planned time.
- `Schedule::Interval` schedules the first run at `now + interval`.
- `max_runs` applies only to interval schedules.
- `MissedRunPolicy::Skip` drops missed occurrences and waits for the next future trigger.
- `MissedRunPolicy::CatchUpOnce` runs one immediate compensating execution for missed occurrences.
- `MissedRunPolicy::ReplayAll` replays each missed occurrence in schedule order.
- `OverlapPolicy::Forbid` skips triggers while a run is active.
- `OverlapPolicy::QueueOne` keeps at most one pending trigger while a run is active.
- `OverlapPolicy::AllowParallel` spawns overlapping runs.
