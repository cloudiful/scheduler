use super::next_after;
use crate::{
    InvalidJobKind, JobTimeWindow, Schedule, SchedulerError, TimeWindowSegment,
    WindowedIntervalSchedule,
};
use chrono::{NaiveTime, TimeZone, Utc};
use std::time::Duration;

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
fn uses_matching_window_frequency() {
    let scheduled_at = Utc.with_ymd_and_hms(2026, 5, 8, 9, 0, 0).unwrap();
    let windowed = WindowedIntervalSchedule::new(Some(Duration::from_secs(1800)))
        .with_window(utc_segment(9, 10), Some(Duration::from_secs(30)));

    let next = next_after(scheduled_at, &windowed, chrono_tz::UTC).unwrap();

    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2026, 5, 8, 9, 0, 30).unwrap())
    );
}

#[test]
fn recomputes_when_candidate_enters_lower_frequency_period() {
    let scheduled_at = Utc.with_ymd_and_hms(2026, 5, 8, 9, 59, 45).unwrap();
    let windowed = WindowedIntervalSchedule::new(Some(Duration::from_secs(1800)))
        .with_window(utc_segment(9, 10), Some(Duration::from_secs(30)));

    let next = next_after(scheduled_at, &windowed, chrono_tz::UTC).unwrap();

    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2026, 5, 8, 10, 30, 15).unwrap())
    );
}

#[test]
fn disabled_default_starts_at_next_enabled_window() {
    let scheduled_at = Utc.with_ymd_and_hms(2026, 5, 8, 8, 0, 0).unwrap();
    let windowed = WindowedIntervalSchedule::new(None)
        .with_window(utc_segment(9, 10), Some(Duration::from_secs(30)));

    let next = next_after(scheduled_at, &windowed, chrono_tz::UTC).unwrap();

    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2026, 5, 8, 9, 0, 0).unwrap())
    );
}

#[test]
fn disabled_window_resumes_at_window_end() {
    let scheduled_at = Utc.with_ymd_and_hms(2026, 5, 8, 11, 45, 0).unwrap();
    let windowed = WindowedIntervalSchedule::new(Some(Duration::from_secs(1800)))
        .with_window(utc_segment(12, 13), None);

    let next = next_after(scheduled_at, &windowed, chrono_tz::UTC).unwrap();

    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2026, 5, 8, 13, 0, 0).unwrap())
    );
}

#[test]
fn supports_cross_midnight_windows() {
    let scheduled_at = Utc.with_ymd_and_hms(2026, 5, 8, 21, 0, 0).unwrap();
    let windowed = WindowedIntervalSchedule::new(None)
        .with_window(utc_segment(22, 2), Some(Duration::from_secs(3600)));

    let next = next_after(scheduled_at, &windowed, chrono_tz::UTC).unwrap();

    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2026, 5, 8, 22, 0, 0).unwrap())
    );

    let next = next_after(
        Utc.with_ymd_and_hms(2026, 5, 9, 1, 30, 0).unwrap(),
        &windowed,
        chrono_tz::UTC,
    )
    .unwrap();

    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2026, 5, 9, 22, 0, 0).unwrap())
    );
}

#[test]
fn first_matching_window_wins() {
    let scheduled_at = Utc.with_ymd_and_hms(2026, 5, 8, 8, 59, 0).unwrap();
    let windowed = WindowedIntervalSchedule::new(Some(Duration::from_secs(1800)))
        .with_window(utc_segment(9, 10), None)
        .with_window(utc_segment(9, 10), Some(Duration::from_secs(30)));

    let next = next_after(scheduled_at, &windowed, chrono_tz::UTC).unwrap();

    assert_eq!(
        next,
        Some(Utc.with_ymd_and_hms(2026, 5, 8, 10, 0, 0).unwrap())
    );
}

#[test]
fn all_disabled_is_noop() {
    let scheduled_at = Utc.with_ymd_and_hms(2026, 5, 8, 8, 59, 0).unwrap();
    let windowed = WindowedIntervalSchedule::new(None).with_window(utc_segment(9, 10), None);

    let next = next_after(scheduled_at, &windowed, chrono_tz::UTC).unwrap();

    assert!(next.is_none());
}

#[test]
fn rejects_zero_frequency() {
    let windowed =
        WindowedIntervalSchedule::new(None).with_window(utc_segment(9, 10), Some(Duration::ZERO));

    let error = next_after(Utc::now(), &windowed, chrono_tz::UTC).unwrap_err();

    assert!(matches!(
        error,
        SchedulerError::InvalidJob(ref invalid)
            if invalid.kind() == InvalidJobKind::ZeroInterval
    ));
}

#[test]
fn schedule_variant_compiles_against_public_type() {
    let schedule = Schedule::WindowedInterval(WindowedIntervalSchedule::new(None));

    assert!(matches!(schedule, Schedule::WindowedInterval(_)));
}
