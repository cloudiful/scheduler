use super::{advance_state_for, collect_due_times, compute_next_after};
use crate::{Job, JobState, MissedRunPolicy, Schedule, StaggeredIntervalSchedule, Task};
use chrono::{TimeDelta, TimeZone, Utc};
use chrono_tz::Asia::Shanghai;
use std::time::Duration;

fn noop_job(schedule: Schedule) -> Job<()> {
    Job::without_deps("job", schedule, Task::from_async(|_| async { Ok(()) }))
}

#[test]
fn compute_next_after_staggered_interval_advances_by_the_base_duration() {
    let scheduled_at = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 0).unwrap();
    let job = noop_job(Schedule::StaggeredInterval(
        StaggeredIntervalSchedule::new(Duration::from_secs(60)).with_seed("site-a"),
    ));

    let next = compute_next_after(&job, scheduled_at, 1, chrono_tz::UTC).unwrap();

    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2026, 4, 3, 1, 3, 0).unwrap())
    );
}

#[test]
fn collect_due_times_replays_staggered_interval_in_fixed_steps() {
    let now = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 3).unwrap();
    let first = (now - TimeDelta::seconds(3)).with_timezone(&Shanghai);
    let mut state = JobState::new("job", Some(first.with_timezone(&Utc)));
    let job = noop_job(Schedule::StaggeredInterval(
        StaggeredIntervalSchedule::new(Duration::from_secs(5)).with_seed("site-a"),
    ))
    .with_missed_run_policy(MissedRunPolicy::ReplayAll);

    let due = collect_due_times(&job, &state, now, Shanghai).unwrap();

    assert_eq!(due, vec![first.with_timezone(&Utc)]);
    advance_state_for(&job, &mut state, &due, Shanghai).unwrap();
    assert_eq!(
        state.next_run_at,
        Some((first + TimeDelta::seconds(5)).with_timezone(&Utc))
    );
}
