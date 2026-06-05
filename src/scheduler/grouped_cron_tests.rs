use super::{initial_next_run_at, next_after, stable_seed_hash, validate};
use crate::{CronSchedule, GroupedCronSchedule, InvalidJobKind, SchedulerError};
use chrono::{TimeDelta, TimeZone, Utc};
use chrono_tz::{Asia::Shanghai, UTC};
use std::time::Duration;

#[test]
fn grouped_cron_initial_next_run_uses_current_anchor_when_due_is_still_future() {
    let schedule = GroupedCronSchedule::new(
        CronSchedule::parse("* * * * *").unwrap(),
        Duration::from_secs(40),
        4,
        3,
    );
    let now = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 20).unwrap();

    let next = initial_next_run_at(now, &schedule, UTC).unwrap();

    assert_eq!(next, Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 30).unwrap());
}

#[test]
fn grouped_cron_initial_next_run_skips_to_next_anchor_when_due_is_past() {
    let schedule = GroupedCronSchedule::new(
        CronSchedule::parse("* * * * *").unwrap(),
        Duration::from_secs(40),
        4,
        1,
    );
    let now = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 20).unwrap();

    let next = initial_next_run_at(now, &schedule, UTC).unwrap();

    assert_eq!(next, Utc.with_ymd_and_hms(2026, 4, 3, 1, 3, 10).unwrap());
}

#[test]
fn grouped_cron_members_are_evenly_spaced_inside_spread() {
    let cron = CronSchedule::parse("* * * * *").unwrap();
    let now = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 0).unwrap();

    let next0 = initial_next_run_at(
        now,
        &GroupedCronSchedule::new(cron.clone(), Duration::from_secs(40), 4, 0),
        UTC,
    )
    .unwrap();
    let next1 = initial_next_run_at(
        now,
        &GroupedCronSchedule::new(cron.clone(), Duration::from_secs(40), 4, 1),
        UTC,
    )
    .unwrap();
    let next2 = initial_next_run_at(
        now,
        &GroupedCronSchedule::new(cron.clone(), Duration::from_secs(40), 4, 2),
        UTC,
    )
    .unwrap();
    let next3 = initial_next_run_at(
        now,
        &GroupedCronSchedule::new(cron, Duration::from_secs(40), 4, 3),
        UTC,
    )
    .unwrap();

    assert_eq!(next0, now + TimeDelta::minutes(1));
    assert_eq!(next1, now + TimeDelta::seconds(10));
    assert_eq!(next2, now + TimeDelta::seconds(20));
    assert_eq!(next3, now + TimeDelta::seconds(30));
}

#[test]
fn grouped_cron_group_seed_rotates_slots_discretely() {
    let cron = CronSchedule::parse("* * * * *").unwrap();
    let now = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 0).unwrap();
    let rotation = (stable_seed_hash("site-a") % 4) as i64;

    let seeded = GroupedCronSchedule::new(cron.clone(), Duration::from_secs(40), 4, 0)
        .with_group_seed("site-a");
    let unseeded = GroupedCronSchedule::new(cron, Duration::from_secs(40), 4, rotation as u32);

    let seeded_next = initial_next_run_at(now, &seeded, UTC).unwrap();
    let rotated_next = initial_next_run_at(now, &unseeded, UTC).unwrap();

    assert_eq!(seeded_next, rotated_next);
}

#[test]
fn grouped_cron_next_after_advances_to_same_slot_on_next_anchor() {
    let schedule = GroupedCronSchedule::new(
        CronSchedule::parse("* * * * *").unwrap(),
        Duration::from_secs(40),
        4,
        2,
    );
    let scheduled_at = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 20).unwrap();

    let next = next_after(scheduled_at, &schedule, UTC).unwrap().unwrap();

    assert_eq!(next, Utc.with_ymd_and_hms(2026, 4, 3, 1, 3, 20).unwrap());
}

#[test]
fn grouped_cron_uses_scheduler_timezone_for_anchor_calculation() {
    let schedule = GroupedCronSchedule::new(
        CronSchedule::parse("0 9 * * *").unwrap(),
        Duration::from_secs(30 * 60),
        3,
        1,
    );
    let start = Utc.with_ymd_and_hms(2026, 4, 3, 0, 30, 0).unwrap();

    let shanghai_next = initial_next_run_at(start, &schedule, Shanghai).unwrap();
    let utc_next = initial_next_run_at(start, &schedule, UTC).unwrap();

    assert_eq!(
        shanghai_next,
        Utc.with_ymd_and_hms(2026, 4, 3, 1, 10, 0).unwrap()
    );
    assert_eq!(utc_next, Utc.with_ymd_and_hms(2026, 4, 3, 9, 10, 0).unwrap());
}

#[test]
fn grouped_cron_validation_rejects_invalid_parameters() {
    let cron = CronSchedule::parse("* * * * *").unwrap();
    let zero_spread = GroupedCronSchedule::new(cron.clone(), Duration::ZERO, 3, 0);
    let zero_group = GroupedCronSchedule::new(cron.clone(), Duration::from_secs(10), 0, 0);
    let out_of_range = GroupedCronSchedule::new(cron, Duration::from_secs(10), 3, 3);

    let zero_spread_error = validate(&zero_spread, UTC, Utc::now()).unwrap_err();
    let zero_group_error = validate(&zero_group, UTC, Utc::now()).unwrap_err();
    let out_of_range_error = validate(&out_of_range, UTC, Utc::now()).unwrap_err();

    assert!(matches!(
        zero_spread_error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
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

#[test]
fn grouped_cron_validation_rejects_spread_that_reaches_next_anchor() {
    let schedule = GroupedCronSchedule::new(
        CronSchedule::parse("* * * * *").unwrap(),
        Duration::from_secs(60),
        4,
        0,
    );

    let error = validate(&schedule, UTC, Utc::now()).unwrap_err();

    assert!(matches!(
        error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::Other
    ));
}
