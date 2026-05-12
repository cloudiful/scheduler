use crate::{JobTimeWindow, TimeWindowAlignment};
use chrono::{DateTime, Datelike, Days, LocalResult, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;

pub(crate) fn align_to_window(
    candidate: Option<DateTime<Utc>>,
    alignment: TimeWindowAlignment,
    window: Option<&JobTimeWindow>,
    fallback_timezone: Tz,
) -> Option<DateTime<Utc>> {
    let candidate = candidate?;
    if !matches!(alignment, TimeWindowAlignment::AlignToNextWindow) {
        return Some(candidate);
    }

    let window = window?;
    if window.matches(candidate, fallback_timezone) {
        return Some(candidate);
    }

    next_matching_start_at_or_after(window, candidate, fallback_timezone)
}

fn next_matching_start_at_or_after(
    window: &JobTimeWindow,
    base: DateTime<Utc>,
    fallback_timezone: Tz,
) -> Option<DateTime<Utc>> {
    boundary_candidates_at_or_after(window, base, fallback_timezone)
        .into_iter()
        .filter(|candidate| *candidate >= base)
        .filter(|candidate| window.matches(*candidate, fallback_timezone))
        .min()
}

fn boundary_candidates_at_or_after(
    window: &JobTimeWindow,
    base: DateTime<Utc>,
    fallback_timezone: Tz,
) -> Vec<DateTime<Utc>> {
    let timezone = window.timezone.unwrap_or(fallback_timezone);
    let local_date = base.with_timezone(&timezone).date_naive();
    let mut candidates = vec![base];

    for offset in 0..=8 {
        let Some(date) = local_date.checked_add_days(Days::new(offset)) else {
            continue;
        };
        push_local_candidate(&mut candidates, timezone, date, NaiveTime::MIN);

        if window.segments.is_empty() {
            if window.weekdays.is_empty() || window.weekdays.contains(&date.weekday()) {
                push_local_candidate(&mut candidates, timezone, date, NaiveTime::MIN);
            }
            continue;
        }

        for segment in &window.segments {
            if window.weekdays.is_empty() || window.weekdays.contains(&date.weekday()) {
                push_local_candidate(&mut candidates, timezone, date, segment.start);
                let end_date = if segment.start <= segment.end {
                    date
                } else if let Some(next_date) = date.checked_add_days(Days::new(1)) {
                    next_date
                } else {
                    continue;
                };
                push_local_candidate(&mut candidates, timezone, end_date, segment.end);
            }
        }
    }

    candidates
}

fn push_local_candidate(
    candidates: &mut Vec<DateTime<Utc>>,
    timezone: Tz,
    date: NaiveDate,
    time: NaiveTime,
) {
    let local = date.and_time(time);
    match timezone.from_local_datetime(&local) {
        LocalResult::Single(value) => candidates.push(value.with_timezone(&Utc)),
        LocalResult::Ambiguous(first, second) => {
            candidates.push(first.with_timezone(&Utc));
            candidates.push(second.with_timezone(&Utc));
        }
        LocalResult::None => {}
    }
}

#[cfg(test)]
#[path = "time_window_alignment_tests.rs"]
mod tests;
