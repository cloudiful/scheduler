use super::{advance_state_for, collect_due_times, compute_next_after};
use crate::{
    Job, JobState, JobTimeWindow, MissedRunPolicy, Schedule, StaggeredIntervalSchedule, Task,
    TimeWindowSegment,
};
use chrono::{NaiveTime, TimeDelta, TimeZone, Utc};
use chrono_tz::Asia::Shanghai;
use std::time::Duration;

fn noop_job(schedule: Schedule) -> Job<()> {
    Job::without_deps("job", schedule, Task::from_async(|_| async { Ok(()) }))
}

fn utc_segment(start_hour: u32, end_hour: u32) -> JobTimeWindow {
    JobTimeWindow {
        timezone: Some(chrono_tz::UTC),
        weekdays: Vec::new(),
        segments: vec![TimeWindowSegment::new(
            NaiveTime::from_hms_opt(start_hour, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(end_hour, 0, 0).unwrap(),
        )],
    }
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

#[test]
fn time_window_alignment_moves_interval_candidate_into_window() {
    let scheduled_at = Utc.with_ymd_and_hms(2026, 5, 8, 9, 45, 0).unwrap();
    let job = noop_job(Schedule::Interval(Duration::from_secs(30 * 60)))
        .with_time_window(utc_segment(9, 10))
        .with_time_window_alignment();

    let next = compute_next_after(&job, scheduled_at, 1, chrono_tz::UTC).unwrap();

    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2026, 5, 9, 9, 0, 0).unwrap())
    );
}

#[test]
fn time_window_alignment_does_not_move_at_times_schedule() {
    let first = Utc.with_ymd_and_hms(2026, 5, 8, 10, 15, 0).unwrap();
    let second = Utc.with_ymd_and_hms(2026, 5, 8, 10, 30, 0).unwrap();
    let job = noop_job(Schedule::AtTimes(vec![
        first.with_timezone(&chrono_tz::UTC),
        second.with_timezone(&chrono_tz::UTC),
    ]))
    .with_time_window(utc_segment(9, 10))
    .with_time_window_alignment();

    let next = compute_next_after(&job, first, 1, chrono_tz::UTC).unwrap();

    assert_eq!(next, Some(second));
}
