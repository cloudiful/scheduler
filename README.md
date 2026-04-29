# cloudiful-scheduler

`cloudiful-scheduler` is a single-job async scheduling library for background ingestion tasks.
The published package name is `cloudiful-scheduler`, while the Rust library import stays `scheduler`.

Links:

- GitHub: <https://github.com/cloudiful/scheduler>
- docs.rs: <https://docs.rs/cloudiful-scheduler>
- crates.io: <https://crates.io/crates/cloudiful-scheduler>

Version `0.3.3` exposes:

- explicit schedules via `Schedule::Interval`, `Schedule::AtTimes`, or `Schedule::Cron`
- missed-run handling via `MissedRunPolicy`
- overlap control via `OverlapPolicy`
- persistent job state via `StateStore`
- optional distributed per-occurrence execution leases via `ExecutionGuard`
- optional terminal-state cleanup via `TerminalStatePolicy`
- optional runtime observation via `SchedulerObserver`
- bounded execution history via `SchedulerReport`

Optional features:

- `valkey-guard` adds `ValkeyExecutionGuard` for Valkey-backed execution leases keyed by `job_id + scheduled_at`.
- `valkey-store` adds `ValkeyStateStore` for Valkey-backed state persistence.
  - `ValkeyStateStore::resilient(...)` permanently downgrades to an in-process mirror after connection-class failures.

The scheduler is responsible only for deciding when to trigger work. Domain-specific recovery, cursor logic, and idempotent refresh commands stay in the caller.
State recovery is keyed only by `job_id`. `StateStore` does not provide distributed locking or leader election by itself; use `ExecutionGuard` when multiple scheduler instances may see the same trigger.

## Add the crate

```toml
[dependencies]
scheduler = { package = "cloudiful-scheduler", version = "0.3.3" }
chrono = "0.4"
chrono-tz = "0.10"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Enable Valkey-backed state persistence:

```toml
[dependencies]
scheduler = { package = "cloudiful-scheduler", version = "0.3.3", features = ["valkey-store"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Enable Valkey-backed execution leases:

```toml
[dependencies]
scheduler = { package = "cloudiful-scheduler", version = "0.3.3", features = ["valkey-guard"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

If you need to consume a tagged GitHub release directly:

```toml
[dependencies]
scheduler = { package = "cloudiful-scheduler", git = "https://github.com/cloudiful/scheduler.git", tag = "v0.3.3" }
```

## Core concepts

- `Scheduler::new(config, store)` creates the runtime.
- `Scheduler::with_observer(config, store, observer)` attaches structured runtime events.
- `Scheduler::with_log_observer(config, store)` adapts runtime events to the `log` crate.
- `Scheduler::with_execution_guard(config, store, guard)` adds distributed per-occurrence mutual exclusion.
- `Scheduler::with_observer_and_execution_guard(config, store, observer, guard)` combines both.
- `Task::from_async(task)` defines an async task from the full `TaskContext`.
- `Task::from_sync(task)` defines a lightweight synchronous task from the full `TaskContext`.
- `Task::from_blocking(task)` defines a blocking synchronous task via `tokio::task::spawn_blocking`.
- `Job::without_deps(job_id, schedule, task)` defines a task with no injected dependencies.
- `Job::new(job_id, schedule, deps, task)` defines a task with explicit injected `deps`.
- `Scheduler::run(job)` runs until the schedule finishes or a control handle requests cancel or shutdown.
- `SchedulerHandle::cancel()` stops while waiting.
- `SchedulerHandle::shutdown()` stops accepting new work and waits for the current run to finish.

Dependency injection in this crate is explicit: you pass a dependency value at job construction time. The scheduler does not auto-resolve arbitrary function parameters.

## Example: async task without dependencies

```rust
use std::time::Duration;

use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, Task};

#[tokio::main]
async fn main() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::without_deps(
        "refresh-cache",
        Schedule::Interval(Duration::from_secs(5)),
        Task::from_async(|_| async move {
            // Call your async command here.
            Ok(())
        }),
    )
    .with_max_runs(1);

    let report = scheduler.run(job).await.unwrap();
    println!("history length: {}", report.history.len());
}
```

## Example: observe runtime events

```rust
use std::time::Duration;

use scheduler::{InMemoryStateStore, Job, LogObserver, Schedule, Scheduler, SchedulerConfig, Task};

#[tokio::main]
async fn main() {
    let scheduler = Scheduler::with_observer(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        LogObserver,
    );

    let job = Job::without_deps(
        "observer-demo",
        Schedule::Interval(Duration::from_secs(5)),
        Task::from_async(|_| async move { Ok(()) }),
    )
    .with_max_runs(1);

    let _ = scheduler.run(job).await.unwrap();
}
```

## Example: cron schedule

Cron expressions use the standard 5-field form: minute, hour, day-of-month, month, day-of-week.
The expression is evaluated in `SchedulerConfig::timezone`.

```rust
use scheduler::{CronSchedule, InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, Task};

#[tokio::main]
async fn main() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::without_deps(
        "market-open-check",
        Schedule::Cron(CronSchedule::parse("*/5 9-15 * * Mon-Fri").unwrap()),
        Task::from_async(|_| async move {
            // Call your async command here.
            Ok(())
        }),
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
    InMemoryStateStore, Job, MissedRunPolicy, OverlapPolicy, Schedule, Scheduler,
    SchedulerConfig, Task,
};

#[tokio::main]
async fn main() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::without_deps(
        "refresh-a-shares",
        Schedule::AtTimes(vec![
            Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(5),
            Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(10),
        ]),
        Task::from_async(|context| async move {
            println!("scheduled for {}", context.run.scheduled_at);
            // Call your idempotent refresh command here.
            Ok(())
        }),
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

use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, Task, TaskContext};

#[derive(Debug)]
struct RefreshDeps {
    market: &'static str,
}

#[tokio::main]
async fn main() {
    let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());

    let job = Job::new(
        "refresh-market",
        Schedule::AtTimes(Vec::new()),
        RefreshDeps { market: "A-share" },
        Task::from_async(|context: TaskContext<RefreshDeps>| async move {
            let deps: Arc<RefreshDeps> = context.deps.clone();
            println!("market: {}", deps.market);
            println!("scheduled for {}", context.run.scheduled_at);
            Ok(())
        }),
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
use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, Task};

#[tokio::main]
async fn main() {
    let store = Arc::new(InMemoryStateStore::new());

    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let job = Job::without_deps(
        "resume-me",
        Schedule::AtTimes(vec![Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(30)]),
        Task::from_async(|_| async move { Ok(()) }),
    );

    let _ = scheduler.run(job).await;

    let scheduler = Scheduler::new(SchedulerConfig::default(), store.clone());
    let job = Job::without_deps(
        "resume-me",
        Schedule::AtTimes(vec![Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(30)]),
        Task::from_async(|_| async move { Ok(()) }),
    );

    let _ = scheduler.run(job).await;
}
```

## Example: Valkey-backed state

This persists `JobState` in a single key per `job_id`. It improves restart recovery, but does not by itself provide distributed locking across multiple scheduler instances.

The current Rust client still uses the `redis://` URI scheme for RESP servers, including Valkey.
By default, keys are written under the `scheduler:valkey:job-state:` prefix. Loads also check the legacy `scheduler:job-state:` prefix so existing persisted state can still be resumed.
For connection-class failures, `ValkeyStateStore::resilient(...)` downgrades to an in-memory mirror. Codec/data errors still fail the run.

```rust
use std::time::Duration;

use scheduler::{Job, Schedule, Scheduler, SchedulerConfig, Task, ValkeyStateStore};

#[tokio::main]
async fn main() {
    let store = ValkeyStateStore::new("redis://127.0.0.1/").await.unwrap();
    let scheduler = Scheduler::new(SchedulerConfig::default(), store);

    let job = Job::without_deps(
        "refresh-cache",
        Schedule::Interval(Duration::from_secs(5)),
        Task::from_async(|_| async move { Ok(()) }),
    )
    .with_max_runs(1);

let report = scheduler.run(job).await.unwrap();
println!("next run: {:?}", report.state.next_run_at);
}
```

## Example: Valkey-backed execution guard

This keeps `StateStore` and distributed mutual exclusion separate. The guard lease key is based on `job_id + scheduled_at`, so `OverlapPolicy::AllowParallel` can still run different occurrences concurrently.

```rust
use std::time::Duration;

use scheduler::{
    InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, Task, ValkeyExecutionGuard,
    ValkeyLeaseConfig,
};

#[tokio::main]
async fn main() {
    let guard = ValkeyExecutionGuard::new(
        "redis://127.0.0.1/",
        ValkeyLeaseConfig {
            ttl: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
        },
    )
    .await
    .unwrap();
    let scheduler = Scheduler::with_execution_guard(
        SchedulerConfig::default(),
        InMemoryStateStore::new(),
        guard,
    );

    let job = Job::without_deps(
        "refresh-cache",
        Schedule::Interval(Duration::from_secs(5)),
        Task::from_async(|_| async move { Ok(()) }),
    )
    .with_max_runs(1);

    let report = scheduler.run(job).await.unwrap();
    println!("history length: {}", report.history.len());
}
```

## Integration test

The Valkey integration tests are marked `ignored` so normal CI stays hermetic. Run them explicitly with a reachable server:

```bash
SCHEDULER_VALKEY_URL=redis://127.0.0.1:6379/ cargo test --features valkey-store --test valkey_store -- --ignored
```

For the execution guard integration tests:

```bash
SCHEDULER_VALKEY_URL=redis://127.0.0.1:6379/ cargo test --features valkey-guard --test execution_guard -- --ignored
```

In Gitea Actions, set the `SCHEDULER_VALKEY_URL` secret to enable the external Valkey integration test step in `.gitea/workflows/ci.yml`.

## Scheduling semantics

- `Schedule::AtTimes` never executes immediately on startup. The first run waits until the first planned time.
- `Schedule::AtTimes(Vec::new())` is a valid no-op schedule and exits without running.
- `Schedule::Interval` schedules the first run at `now + interval`.
- `Schedule::Cron` evaluates a standard 5-field expression in `SchedulerConfig::timezone`.
- `max_runs` applies to interval schedules, explicit `AtTimes` schedules, and cron schedules.
- `with_max_runs(0)` exits immediately without running any task.
- `Job::new_sync*` is for lightweight synchronous logic. Use `Job::new_blocking*` for blocking I/O or CPU-heavy synchronous work.
- `MissedRunPolicy::Skip` drops missed occurrences and waits for the next future trigger.
- `MissedRunPolicy::CatchUpOnce` runs one immediate compensating execution for missed occurrences.
- `MissedRunPolicy::ReplayAll` replays each missed occurrence in schedule order.
- `OverlapPolicy::Forbid` skips triggers while a run is active.
- `OverlapPolicy::QueueOne` keeps at most one pending trigger while a run is active.
- `OverlapPolicy::AllowParallel` spawns overlapping runs.
- `SchedulerConfig::timezone` is forwarded to `RunContext`, drives cron evaluation, and does not rewrite absolute `AtTimes` timestamps.
- State recovery is keyed by `job_id`; restarting with the same `job_id` resumes from the stored `next_run_at`.
- `ExecutionGuard` is keyed by trigger occurrence, not only by `job_id`, so different `scheduled_at` values can acquire separate leases.
- Corrupted persisted interval state with `next_run_at = None` is repaired automatically if the job is not actually terminal.
- `SchedulerConfig::terminal_state_policy = Delete` removes persisted terminal state once the job finishes.
- `ResilientStateStore` masks connection-class failures by switching permanently to its in-process mirror; degradation can be observed via `SchedulerObserver`.
- `StateStore` and `ExecutionGuard` are intentionally separate: state persistence does not imply distributed mutual exclusion.
