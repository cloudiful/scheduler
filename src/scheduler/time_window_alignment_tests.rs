use super::align_to_window;
use crate::{JobTimeWindow, TimeWindowAlignment, TimeWindowSegment};
use chrono::{NaiveTime, TimeZone, Utc, Weekday};

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
fn disabled_alignment_returns_candidate_unchanged() {
    let candidate = Utc.with_ymd_and_hms(2026, 5, 8, 8, 0, 0).unwrap();
    let aligned = align_to_window(
        Some(candidate),
        TimeWindowAlignment::Disabled,
        Some(&utc_segment(9, 10)),
        chrono_tz::UTC,
    );

    assert_eq!(aligned, Some(candidate));
}

#[test]
fn candidate_inside_window_is_unchanged() {
    let candidate = Utc.with_ymd_and_hms(2026, 5, 8, 9, 30, 0).unwrap();
    let aligned = align_to_window(
        Some(candidate),
        TimeWindowAlignment::AlignToNextWindow,
        Some(&utc_segment(9, 10)),
        chrono_tz::UTC,
    );

    assert_eq!(aligned, Some(candidate));
}

#[test]
fn candidate_outside_window_moves_to_next_start() {
    let candidate = Utc.with_ymd_and_hms(2026, 5, 8, 10, 15, 0).unwrap();
    let aligned = align_to_window(
        Some(candidate),
        TimeWindowAlignment::AlignToNextWindow,
        Some(&utc_segment(9, 10)),
        chrono_tz::UTC,
    );

    assert_eq!(
        aligned,
        Some(Utc.with_ymd_and_hms(2026, 5, 9, 9, 0, 0).unwrap())
    );
}

#[test]
fn weekday_only_window_moves_to_next_allowed_day() {
    let candidate = Utc.with_ymd_and_hms(2026, 5, 9, 10, 15, 0).unwrap();
    let window = JobTimeWindow {
        timezone: Some(chrono_tz::UTC),
        weekdays: vec![Weekday::Mon],
        segments: Vec::new(),
    };

    let aligned = align_to_window(
        Some(candidate),
        TimeWindowAlignment::AlignToNextWindow,
        Some(&window),
        chrono_tz::UTC,
    );

    assert_eq!(
        aligned,
        Some(Utc.with_ymd_and_hms(2026, 5, 11, 0, 0, 0).unwrap())
    );
}
