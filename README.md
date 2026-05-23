# cloudiful-scheduler

`cloudiful-scheduler` is a single-job async scheduling library for background ingestion tasks.
The published package name is `cloudiful-scheduler`, while the Rust library import stays `scheduler`.

Links:

- GitHub: <https://github.com/cloudiful/scheduler>
- docs.rs: <https://docs.rs/cloudiful-scheduler>
- crates.io: <https://crates.io/crates/cloudiful-scheduler>

Version `0.4.5` exposes:

- explicit schedules via `Schedule::Interval`, `Schedule::StaggeredInterval`, `Schedule::GroupedInterval`, `Schedule::AtTimes`, or `Schedule::Cron`
- windowed interval schedules via `Schedule::WindowedInterval` for different frequencies across local time windows
- job-level execution windows via `JobTimeWindow`
- missed-run handling via `MissedRunPolicy`
- overlap control via `OverlapPolicy`
- persistent job state via `StateStore`
- optional distributed per-occurrence execution leases via `ExecutionGuard`
- optional coordinated runtime state via `CoordinatedStateStore`
- optional terminal-state cleanup via `TerminalStatePolicy`
- optional runtime observation via `SchedulerObserver`
- bounded execution history via `SchedulerReport`
- pause / resume control via `SchedulerHandle`

Optional features:

- `valkey-guard` adds `ValkeyExecutionGuard` for Valkey-backed execution leases keyed by `job_id + scheduled_at`.
- `valkey-store` adds `ValkeyStateStore` for Valkey-backed state persistence.
  - `ValkeyCoordinatedStateStore` for shared pause/resume state plus expired inflight reclaim across scheduler instances.
  - `ValkeyStateStore::resilient(...)` permanently downgrades to an in-process mirror after connection-class failures.

The scheduler is responsible only for deciding when to trigger work. Domain-specific recovery, cursor logic, and idempotent refresh commands stay in the caller.
State recovery is keyed only by `job_id`. `StateStore` does not provide distributed locking or leader election by itself; use `ExecutionGuard` when multiple scheduler instances may see the same trigger.

## Add the crate

```toml
[dependencies]
scheduler = { package = "cloudiful-scheduler", version = "0.4.3" }
chrono = "0.4"
chrono-tz = "0.10"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Enable Valkey-backed state persistence:

```toml
[dependencies]
scheduler = { package = "cloudiful-scheduler", version = "0.4.3", features = ["valkey-store"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

If multiple scheduler instances must share pause state and reclaim expired
inflight occurrences, use `ValkeyCoordinatedStateStore` together with
`Scheduler::with_coordinated_state_store(...)`. During connection-class Valkey
failures, coordinated scheduling fails closed: it does not claim new triggers
or advance shared state until Valkey accepts commands again, then reloads
shared state and resumes normal claim/reclaim flow.

Enable Valkey-backed execution leases:

```toml
[dependencies]
scheduler = { package = "cloudiful-scheduler", version = "0.4.3", features = ["valkey-guard"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

If you need to consume a tagged GitHub release directly:

```toml
[dependencies]
scheduler = { package = "cloudiful-scheduler", git = "https://github.com/cloudiful/scheduler.git", tag = "v0.4.3" }
```

## Core concepts

- `Scheduler::new(config, store)` creates the runtime.
- `Scheduler::with_observer(config, store, observer)` attaches structured runtime events.
- `Scheduler::with_log_observer(config, store)` adapts runtime events to the `log` crate.
- `Scheduler::with_execution_guard(config, store, guard)` adds distributed per-occurrence mutual exclusion.
- `Scheduler::with_observer_and_execution_guard(config, store, observer, guard)` combines both.
- `Scheduler::with_coordinated_state_store(config, store, lease_config)` enables shared coordinated scheduling on top of a coordinated state store.
- `Task::from_async(task)` defines an async task from the full `TaskContext`.
- `Task::from_sync(task)` defines a lightweight synchronous task from the full `TaskContext`.
- `Task::from_blocking(task)` defines a blocking synchronous task via `tokio::task::spawn_blocking`.
- `Job::without_deps(job_id, schedule, task)` defines a task with no injected dependencies.
- `Job::new(job_id, schedule, deps, task)` defines a task with explicit injected `deps`.
- `Job::with_time_window(window)` restricts execution to local weekdays and time segments.
- `Job::with_time_window_alignment()` shifts interval-style schedule candidates forward into the configured time window.
- `Schedule::windowed_interval(default_every)` selects interval frequency by ordered `JobTimeWindow` rules; `None` means no triggers for that period.
- `Scheduler::run(job)` runs until the schedule finishes or a control handle requests cancel or shutdown.
- `SchedulerHandle::cancel()` stops while waiting.
- `SchedulerHandle::shutdown()` stops accepting new work and waits for the current run to finish.
- `SchedulerHandle::pause().await` stops future triggers without interrupting the current run.
- `SchedulerHandle::resume().await` wakes the scheduler immediately and resumes with the configured missed-run policy.
- `SchedulerHandle::trigger_now().await` submits one immediate manual trigger through the normal scheduler execution path.

Dependency injection in this crate is explicit: you pass a dependency value at job construction time. The scheduler does not auto-resolve arbitrary function parameters.

Manual trigger notes:

- `trigger_now()` is a submission API, not a synchronous execution API.
- It does not rewrite the configured schedule or `next_run_at`; it adds one immediate trigger.
- It uses the existing overlap policy, queueing behavior, execution guard, observer, store, and coordinated claim/complete path.
- If the scheduler is paused, the manual trigger is accepted and runs after `resume()`.
- `SchedulerHandle` still assumes one logical active job per scheduler instance; `trigger_now()` requires exactly one active job on that handle.
- Tasks that need to distinguish scheduled vs manual execution can use `Task::from_*_with_trigger(...)` and inspect `TriggeredTaskContext::source`.

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

## Example: staggered daily scraper

```rust
use std::time::Duration;

use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, Task};

let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
let job = Job::without_deps(
    "scrape-example",
    Schedule::staggered_interval_with_seed(Duration::from_secs(24 * 60 * 60), "example.com"),
    Task::from_async(|_| async move {
        // Fetch pages for the same upstream without starting every job at once.
        Ok(())
    }),
);
```

## Example: grouped daily scraper set

```rust
use std::time::Duration;

use scheduler::{InMemoryStateStore, Job, Schedule, Scheduler, SchedulerConfig, Task};

let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
let job = Job::without_deps(
    "scrape-example-1",
    Schedule::grouped_interval_with_seed(Duration::from_secs(24 * 60 * 60), 3, 1, "example.com"),
    Task::from_async(|_| async move {
        // Members in the same group spread evenly across the interval.
        Ok(())
    }),
);
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

## Example: job execution window

```rust
use chrono::{NaiveTime, TimeDelta, Utc, Weekday};
use chrono_tz::Asia::Shanghai;
use scheduler::{InMemoryStateStore, Job, JobTimeWindow, Schedule, Scheduler, SchedulerConfig, Task, TimeWindowSegment};

let window = JobTimeWindow {
    timezone: Some(Shanghai),
    weekdays: vec![Weekday::Mon, Weekday::Tue, Weekday::Wed, Weekday::Thu, Weekday::Fri],
    segments: vec![
        TimeWindowSegment::new(
            NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        ),
        TimeWindowSegment::new(
            NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(18, 0, 0).unwrap(),
        ),
    ],
};

let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
let job = Job::without_deps(
    "windowed-job",
    Schedule::AtTimes(vec![Utc::now().with_timezone(&Shanghai) + TimeDelta::seconds(5)]),
    Task::from_async(|_| async move { Ok(()) }),
)
    .with_time_window(window);

let report = scheduler.run(job).await.unwrap();
println!("skip reason: {:?}", report.last_skip_reason);
```

Use `with_time_window_alignment()` when interval, staggered interval, or grouped
interval schedules should move their trigger time into the execution window
instead of emitting an outside-window trigger that will be skipped.

## Example: windowed interval frequencies

Use `WindowedIntervalSchedule` when one job needs different trigger rates in
different local periods. A matching window with `None` disables triggers for
that period; `Job::with_time_window(...)` remains a separate hard execution
filter.

```rust
use std::time::Duration;

use chrono::{NaiveTime, Weekday};
use chrono_tz::Asia::Shanghai;
use scheduler::{
    InMemoryStateStore, Job, JobTimeWindow, Schedule, Scheduler, SchedulerConfig, Task,
    TimeWindowSegment, WindowedIntervalSchedule,
};

let market_open = JobTimeWindow {
    timezone: Some(Shanghai),
    weekdays: vec![Weekday::Mon, Weekday::Tue, Weekday::Wed, Weekday::Thu, Weekday::Fri],
    segments: vec![
        TimeWindowSegment::new(
            NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
        ),
        TimeWindowSegment::new(
            NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
        ),
    ],
};

let schedule = WindowedIntervalSchedule::new(Some(Duration::from_secs(30 * 60)))
    .with_window(market_open, Some(Duration::from_secs(30)));

let scheduler = Scheduler::new(SchedulerConfig::default(), InMemoryStateStore::new());
let job = Job::without_deps(
    "refresh-stocks",
    Schedule::WindowedInterval(schedule),
    Task::from_async(|_| async move {
        // Fetch frequently during market hours and less often outside.
        Ok(())
    }),
);
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
`pause()/resume()` follow the same split: legacy schedulers pause only the local instance, while coordinated schedulers persist a shared paused flag per `job_id`.

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
Valkey commands use a small retry/backoff window through `ValkeyRecoveryConfig`.
For connection-class failures, `ValkeyStateStore::resilient(...)` downgrades to an in-memory mirror, records dirty state, and writes it back once Valkey accepts commands again. Codec/data errors still fail the run.

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
When Valkey is temporarily unavailable, the guard fails closed: new runs are treated as contended after retry/backoff instead of using a local lock. If renewal fails for an existing lease, the scheduler reports the lease as lost and stops future triggers for that run.

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

For the script-level execution guard semantics:

```bash
SCHEDULER_VALKEY_URL=redis://127.0.0.1:6379/ cargo test --features valkey-store,valkey-guard --test valkey_guard_scripts -- --ignored --nocapture
```

For the coordinated runtime store scripts:

```bash
SCHEDULER_VALKEY_URL=redis://127.0.0.1:6379/ cargo test --features valkey-store,valkey-guard --test valkey_coordinated_scripts -- --ignored --nocapture
```

For the scheduler-level multi-instance guard integration:

```bash
SCHEDULER_VALKEY_URL=redis://127.0.0.1:6379/ cargo test --features valkey-guard --test execution_guard -- --ignored --nocapture
```

In Gitea Actions, set the `SCHEDULER_VALKEY_URL` secret to enable the external Valkey integration test step in `.gitea/workflows/ci.yml`.

## Scheduling semantics

- `Schedule::AtTimes` never executes immediately on startup. The first run waits until the first planned time.
- `Schedule::AtTimes(Vec::new())` is a valid no-op schedule and exits without running.
- `Schedule::Interval` schedules the first run at `now + interval`.
- `Schedule::StaggeredInterval` uses a stable phase derived from the job id or an explicit seed, then repeats by the base interval.
- `Schedule::GroupedInterval` spreads a fixed-size group evenly across the interval, then repeats by the base interval.
- `Schedule::Cron` evaluates a standard 5-field expression in `SchedulerConfig::timezone`.
- `max_runs` applies to interval schedules, staggered/grouped interval schedules, explicit `AtTimes` schedules, and cron schedules.
- `with_max_runs(0)` exits immediately without running any task.
- `Task::from_sync` is for lightweight synchronous logic. Use `Task::from_blocking` for blocking I/O or CPU-heavy synchronous work.
- `MissedRunPolicy::Skip` drops missed occurrences and waits for the next future trigger.
- `MissedRunPolicy::CatchUpOnce` runs one immediate compensating execution for missed occurrences.
- `MissedRunPolicy::ReplayAll` replays each missed occurrence in schedule order.
- `OverlapPolicy::Forbid` skips triggers while a run is active.
- `OverlapPolicy::QueueOne` keeps at most one pending trigger while a run is active.
- `OverlapPolicy::AllowParallel` spawns overlapping runs.
- `JobTimeWindow` is checked at execution time; outside-window occurrences are consumed, skipped, and reported as `outside_time_window`.
- `SchedulerConfig::timezone` is forwarded to `RunContext`, drives cron evaluation, and does not rewrite absolute `AtTimes` timestamps.
- State recovery is keyed by `job_id`; restarting with the same `job_id` resumes from the stored `next_run_at`.
- `ExecutionGuard` is keyed by trigger occurrence, not only by `job_id`, so different `scheduled_at` values can acquire separate leases.
- Corrupted persisted interval state with `next_run_at = None` is repaired automatically if the job is not actually terminal.
- `SchedulerConfig::terminal_state_policy = Delete` removes persisted terminal state once the job finishes.
- `ResilientStateStore` masks connection-class failures by switching permanently to its in-process mirror; degradation can be observed via `SchedulerObserver`.
- `StateStore` and `ExecutionGuard` are intentionally separate: state persistence does not imply distributed mutual exclusion.
- Jobs without `JobTimeWindow` keep the existing behavior.
