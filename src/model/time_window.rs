use chrono::{DateTime, Datelike, NaiveTime, Utc, Weekday};
use chrono_tz::Tz;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunSkipReason {
    OutsideTimeWindow,
}

impl RunSkipReason {
    pub fn as_str(self) -> &'static str {
        match self {
            RunSkipReason::OutsideTimeWindow => "outside_time_window",
        }
    }
}

impl std::fmt::Display for RunSkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeWindowSegment {
    pub start: NaiveTime,
    pub end: NaiveTime,
}

impl TimeWindowSegment {
    pub fn new(start: NaiveTime, end: NaiveTime) -> Self {
        Self { start, end }
    }

    fn contains_time(&self, time: NaiveTime) -> bool {
        if self.start <= self.end {
            time >= self.start && time < self.end
        } else {
            time >= self.start || time < self.end
        }
    }

    fn effective_weekday(&self, weekday: Weekday, time: NaiveTime) -> Weekday {
        if self.start <= self.end || time >= self.start {
            weekday
        } else {
            previous_weekday(weekday)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JobTimeWindow {
    pub timezone: Option<Tz>,
    pub weekdays: Vec<Weekday>,
    pub segments: Vec<TimeWindowSegment>,
}

impl JobTimeWindow {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn matches(&self, now: DateTime<Utc>, fallback_timezone: Tz) -> bool {
        let timezone = self.timezone.unwrap_or(fallback_timezone);
        let local = now.with_timezone(&timezone);
        self.matches_local(local)
    }

    fn matches_local(&self, local: DateTime<Tz>) -> bool {
        let weekday = local.weekday();
        let time = local.time();

        if self.segments.is_empty() {
            return self.weekdays.is_empty() || self.weekdays.contains(&weekday);
        }

        self.segments.iter().any(|segment| {
            if !segment.contains_time(time) {
                return false;
            }

            self.weekdays.is_empty()
                || self
                    .weekdays
                    .contains(&segment.effective_weekday(weekday, time))
        })
    }
}

fn previous_weekday(weekday: Weekday) -> Weekday {
    match weekday {
        Weekday::Mon => Weekday::Sun,
        Weekday::Tue => Weekday::Mon,
        Weekday::Wed => Weekday::Tue,
        Weekday::Thu => Weekday::Wed,
        Weekday::Fri => Weekday::Thu,
        Weekday::Sat => Weekday::Fri,
        Weekday::Sun => Weekday::Sat,
    }
}

#[cfg(test)]
mod tests {
    use super::{JobTimeWindow, RunSkipReason, TimeWindowSegment};
    use chrono::{NaiveTime, TimeZone, Utc, Weekday};
    use chrono_tz::{Asia::Shanghai, UTC};

    #[test]
    fn fallback_timezone_is_used_when_window_timezone_is_missing() {
        let window = JobTimeWindow {
            timezone: None,
            weekdays: vec![Weekday::Sat],
            segments: vec![TimeWindowSegment::new(
                NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(1, 0, 0).unwrap(),
            )],
        };
        let instant = Utc.with_ymd_and_hms(2026, 5, 8, 16, 30, 0).unwrap();

        assert!(!window.matches(instant, UTC));
        assert!(window.matches(instant, Shanghai));
    }

    #[test]
    fn same_day_segments_match_by_local_time() {
        let window = JobTimeWindow {
            timezone: Some(UTC),
            weekdays: vec![Weekday::Fri],
            segments: vec![
                TimeWindowSegment::new(
                    NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                    NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
                ),
                TimeWindowSegment::new(
                    NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
                    NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
                ),
            ],
        };

        assert!(window.matches(Utc.with_ymd_and_hms(2026, 5, 8, 13, 30, 0).unwrap(), UTC));
        assert!(!window.matches(Utc.with_ymd_and_hms(2026, 5, 8, 11, 30, 0).unwrap(), UTC));
    }

    #[test]
    fn cross_midnight_segments_use_the_effective_local_day() {
        let window = JobTimeWindow {
            timezone: Some(UTC),
            weekdays: vec![Weekday::Thu],
            segments: vec![TimeWindowSegment::new(
                NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(2, 0, 0).unwrap(),
            )],
        };

        assert!(window.matches(Utc.with_ymd_and_hms(2026, 5, 8, 1, 0, 0).unwrap(), UTC));
        assert!(!window.matches(Utc.with_ymd_and_hms(2026, 5, 8, 3, 0, 0).unwrap(), UTC));
    }

    #[test]
    fn run_skip_reason_has_expected_string_form() {
        assert_eq!(
            RunSkipReason::OutsideTimeWindow.as_str(),
            "outside_time_window"
        );
        assert_eq!(
            RunSkipReason::OutsideTimeWindow.to_string(),
            "outside_time_window"
        );
    }
}
