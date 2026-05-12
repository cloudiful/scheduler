use super::{
    grouped_initial_next_run_at, nanos_to_utc, stable_seed_hash, staggered_initial_next_run_at,
    utc_to_nanos, validate_grouped_interval,
};
use crate::{GroupedIntervalSchedule, InvalidJobKind, SchedulerError, StaggeredIntervalSchedule};
use chrono::{TimeDelta, TimeZone, Utc};
use std::time::Duration;

#[test]
fn staggered_initial_next_run_is_deterministic_and_bounded() {
    let now = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 3).unwrap();
    let schedule = StaggeredIntervalSchedule::new(Duration::from_secs(86_400));
    let seeded = schedule.clone().with_seed("site-a");
    let next = staggered_initial_next_run_at(now, &seeded, "job-a").unwrap();

    let interval_nanos = 86_400_u128 * 1_000_000_000;
    let phase_nanos = i128::from(stable_seed_hash("site-a")).rem_euclid(interval_nanos as i128);
    let now_nanos = utc_to_nanos(now);
    let cycle_start = now_nanos.div_euclid(interval_nanos as i128) * interval_nanos as i128;
    let mut expected = cycle_start + phase_nanos;
    if expected < now_nanos {
        expected += interval_nanos as i128;
    }

    assert_eq!(next, nanos_to_utc(expected).unwrap());
    assert_eq!(
        staggered_initial_next_run_at(now, &schedule.clone().with_seed("site-a"), "job-a").unwrap(),
        next
    );
    assert!(next >= now);
    assert!(next < now + TimeDelta::seconds(86_400));
}

#[test]
fn staggered_initial_next_run_falls_back_to_job_id_when_seed_missing() {
    let now = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 3).unwrap();
    let schedule = StaggeredIntervalSchedule::new(Duration::from_secs(3_600));

    let next = staggered_initial_next_run_at(now, &schedule, "job-a").unwrap();
    let repeated = staggered_initial_next_run_at(now, &schedule, "job-a").unwrap();
    let different = staggered_initial_next_run_at(now, &schedule, "job-b").unwrap();

    assert_eq!(next, repeated);
    assert!(next >= now);
    assert!(next < now + TimeDelta::seconds(3_600));
    assert_ne!(next, different);
}

#[test]
fn grouped_initial_next_run_is_evenly_spaced() {
    let now = Utc.with_ymd_and_hms(2026, 4, 3, 0, 0, 0).unwrap();
    let interval = Duration::from_secs(120 * 60);
    let member0 = GroupedIntervalSchedule::new(interval, 3, 0);
    let member1 = GroupedIntervalSchedule::new(interval, 3, 1);
    let member2 = GroupedIntervalSchedule::new(interval, 3, 2);

    let next0 = grouped_initial_next_run_at(now, &member0).unwrap();
    let next1 = grouped_initial_next_run_at(now, &member1).unwrap();
    let next2 = grouped_initial_next_run_at(now, &member2).unwrap();

    assert_eq!(next0, now);
    assert_eq!(next1, now + TimeDelta::minutes(40));
    assert_eq!(next2, now + TimeDelta::minutes(80));
}

#[test]
fn grouped_initial_next_run_uses_group_seed_as_a_shared_rotation() {
    let now = Utc.with_ymd_and_hms(2026, 4, 3, 0, 0, 0).unwrap();
    let interval = Duration::from_secs(120 * 60);
    let a0 = GroupedIntervalSchedule::new(interval, 3, 0).with_group_seed("site-a");
    let a1 = GroupedIntervalSchedule::new(interval, 3, 1).with_group_seed("site-a");
    let b0 = GroupedIntervalSchedule::new(interval, 3, 0).with_group_seed("site-b");

    let next_a0 = grouped_initial_next_run_at(now, &a0).unwrap();
    let next_a1 = grouped_initial_next_run_at(now, &a1).unwrap();
    let next_b0 = grouped_initial_next_run_at(now, &b0).unwrap();

    assert_eq!(
        (next_a1 - next_a0).num_seconds().rem_euclid(120 * 60),
        40 * 60
    );
    assert_eq!(next_a0, grouped_initial_next_run_at(now, &a0).unwrap());
    assert_ne!(next_a0, next_b0);
}

#[test]
fn grouped_interval_validation_rejects_invalid_parameters() {
    let zero_group = GroupedIntervalSchedule::new(Duration::from_secs(60), 0, 0);
    let out_of_range = GroupedIntervalSchedule::new(Duration::from_secs(60), 3, 3);

    let zero_group_error = validate_grouped_interval(&zero_group).unwrap_err();
    let out_of_range_error = validate_grouped_interval(&out_of_range).unwrap_err();

    assert!(matches!(
        zero_group_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
    assert!(matches!(
        out_of_range_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
}
