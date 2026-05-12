use crate::error::SchedulerError;
use crate::{WindowedIntervalSchedule, scheduler::trigger_math::duration_to_delta};
use chrono::{DateTime, Datelike, Days, LocalResult, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use std::time::Duration;

pub(crate) fn validate(windowed: &WindowedIntervalSchedule) -> Result<(), SchedulerError> {
    if matches!(windowed.default_every, Some(every) if every.is_zero()) {
        return Err(SchedulerError::invalid_zero_interval());
    }
    for window in &windowed.windows {
        if matches!(window.every, Some(every) if every.is_zero()) {
            return Err(SchedulerError::invalid_zero_interval());
        }
    }
    Ok(())
}

pub(crate) fn next_after(
    base: DateTime<Utc>,
    windowed: &WindowedIntervalSchedule,
    fallback_timezone: Tz,
) -> Result<Option<DateTime<Utc>>, SchedulerError> {
    validate(windowed)?;

    let Some(mut current_every) = active_interval_at(windowed, base, fallback_timezone) else {
        return Ok(next_enabled_start_after(windowed, base, fallback_timezone));
    };

    let delta = duration_to_delta(current_every)
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
    let mut candidate = base
        .checked_add_signed(delta)
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;

    for _ in 0..32 {
        match active_interval_at(windowed, candidate, fallback_timezone) {
            Some(next_every) if next_every == current_every => return Ok(Some(candidate)),
            Some(next_every) => {
                let delta = duration_to_delta(next_every)
                    .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
                candidate = candidate
                    .checked_add_signed(delta)
                    .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
                current_every = next_every;
            }
            None => {
                return Ok(next_enabled_start_after(
                    windowed,
                    candidate,
                    fallback_timezone,
                ));
            }
        }
    }

    Ok(next_enabled_start_after(
        windowed,
        candidate,
        fallback_timezone,
    ))
}

fn active_interval_at(
    windowed: &WindowedIntervalSchedule,
    instant: DateTime<Utc>,
    fallback_timezone: Tz,
) -> Option<Duration> {
    for interval_window in &windowed.windows {
        if interval_window.window.matches(instant, fallback_timezone) {
            return interval_window.every;
        }
    }
    windowed.default_every
}

fn next_enabled_start_after(
    windowed: &WindowedIntervalSchedule,
    base: DateTime<Utc>,
    fallback_timezone: Tz,
) -> Option<DateTime<Utc>> {
    boundary_candidates_after(windowed, base, fallback_timezone)
        .into_iter()
        .filter(|candidate| *candidate > base)
        .filter(|candidate| active_interval_at(windowed, *candidate, fallback_timezone).is_some())
        .min()
}

fn boundary_candidates_after(
    windowed: &WindowedIntervalSchedule,
    base: DateTime<Utc>,
    fallback_timezone: Tz,
) -> Vec<DateTime<Utc>> {
    let mut candidates = Vec::new();
    for interval_window in &windowed.windows {
        let timezone = interval_window.window.timezone.unwrap_or(fallback_timezone);
        let local_date = base.with_timezone(&timezone).date_naive();
        for offset in 0..=8 {
            let Some(date) = local_date.checked_add_days(Days::new(offset)) else {
                continue;
            };
            push_local_candidate(&mut candidates, timezone, date, NaiveTime::MIN);

            if interval_window.window.segments.is_empty() {
                if interval_window.window.weekdays.is_empty()
                    || interval_window.window.weekdays.contains(&date.weekday())
                {
                    push_local_candidate(&mut candidates, timezone, date, NaiveTime::MIN);
                    if let Some(next_date) = date.checked_add_days(Days::new(1)) {
                        push_local_candidate(&mut candidates, timezone, next_date, NaiveTime::MIN);
                    }
                }
                continue;
            }

            for segment in &interval_window.window.segments {
                if interval_window.window.weekdays.is_empty()
                    || interval_window.window.weekdays.contains(&date.weekday())
                {
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
#[path = "windowed_interval_tests.rs"]
mod tests;
