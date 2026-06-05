use crate::error::SchedulerError;
use crate::model::CronSchedule;
use crate::{GroupedCronSchedule, InvalidJobKind};
use chrono::{DateTime, TimeDelta, Utc};
use chrono_tz::Tz;
use std::time::Duration;

pub(crate) fn initial_next_run_at(
    now: DateTime<Utc>,
    grouped: &GroupedCronSchedule,
    timezone: Tz,
) -> Result<DateTime<Utc>, SchedulerError> {
    validate(grouped, timezone, now)?;

    let lower_bound = now
        .checked_sub_signed(duration_to_delta(grouped.spread)?)
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
    if let Some(current_anchor) = anchor_in_window(lower_bound, now, &grouped.cron, timezone)?
    {
        let current_due = due_at_anchor(current_anchor, grouped)?;
        if current_due > now {
            return Ok(current_due);
        }
    }

    let next_anchor = grouped
        .cron
        .next_after(now, timezone)
        .ok_or_else(|| invalid_grouped_cron("grouped cron could not compute next anchor"))?;
    validate_anchor_gap(grouped, timezone, next_anchor)?;
    due_at_anchor(next_anchor, grouped)
}

pub(crate) fn next_after(
    scheduled_at: DateTime<Utc>,
    grouped: &GroupedCronSchedule,
    timezone: Tz,
) -> Result<Option<DateTime<Utc>>, SchedulerError> {
    validate(grouped, timezone, scheduled_at)?;

    let anchor = anchor_from_scheduled_at(scheduled_at, grouped)?;
    validate_anchor_gap(grouped, timezone, anchor)?;

    let next_anchor = grouped
        .cron
        .next_after(anchor, timezone)
        .ok_or_else(|| invalid_grouped_cron("grouped cron could not compute next anchor"))?;
    validate_anchor_gap(grouped, timezone, next_anchor)?;

    due_at_anchor(next_anchor, grouped).map(Some)
}

pub(crate) fn validate(
    grouped: &GroupedCronSchedule,
    timezone: Tz,
    reference: DateTime<Utc>,
) -> Result<(), SchedulerError> {
    if grouped.spread.is_zero() {
        return Err(invalid_grouped_cron(
            "grouped cron spread must be greater than zero",
        ));
    }
    if grouped.group_size == 0 {
        return Err(invalid_grouped_cron(
            "grouped cron group_size must be greater than zero",
        ));
    }
    if grouped.member_index >= grouped.group_size {
        return Err(invalid_grouped_cron(
            "grouped cron member_index must be less than group_size",
        ));
    }

    let next_anchor = grouped
        .cron
        .next_after(reference, timezone)
        .ok_or_else(|| invalid_grouped_cron("grouped cron could not compute next anchor"))?;
    validate_anchor_gap(grouped, timezone, next_anchor)?;
    Ok(())
}

fn validate_anchor_gap(
    grouped: &GroupedCronSchedule,
    timezone: Tz,
    anchor: DateTime<Utc>,
) -> Result<(), SchedulerError> {
    let next_anchor = grouped
        .cron
        .next_after(anchor, timezone)
        .ok_or_else(|| invalid_grouped_cron("grouped cron could not compute anchor gap"))?;
    let gap = next_anchor - anchor;
    let spread = duration_to_delta(grouped.spread)?;
    if spread >= gap {
        return Err(invalid_grouped_cron(
            "grouped cron spread must be smaller than the next cron anchor gap",
        ));
    }
    Ok(())
}

fn due_at_anchor(
    anchor: DateTime<Utc>,
    grouped: &GroupedCronSchedule,
) -> Result<DateTime<Utc>, SchedulerError> {
    let offset = slot_offset(grouped)?;
    anchor
        .checked_add_signed(offset)
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)
}

fn anchor_from_scheduled_at(
    scheduled_at: DateTime<Utc>,
    grouped: &GroupedCronSchedule,
) -> Result<DateTime<Utc>, SchedulerError> {
    let offset = slot_offset(grouped)?;
    scheduled_at
        .checked_sub_signed(offset)
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)
}

fn slot_offset(grouped: &GroupedCronSchedule) -> Result<TimeDelta, SchedulerError> {
    let spread_nanos = duration_to_nanos(grouped.spread)
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
    let rotation = grouped
        .group_seed
        .as_deref()
        .map(stable_seed_hash)
        .map(|hash| hash % u64::from(grouped.group_size))
        .unwrap_or(0) as u32;
    let slot_index = (grouped.member_index + rotation) % grouped.group_size;
    let offset_nanos = spread_nanos
        .checked_mul(u128::from(slot_index))
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)?
        / u128::from(grouped.group_size);
    nanos_to_delta(offset_nanos)
}

fn duration_to_delta(duration: Duration) -> Result<TimeDelta, SchedulerError> {
    TimeDelta::from_std(duration).map_err(|_| SchedulerError::invalid_interval_out_of_range())
}

fn duration_to_nanos(duration: Duration) -> Option<u128> {
    let nanos = u128::from(duration.as_secs())
        .checked_mul(1_000_000_000)?
        .checked_add(u128::from(duration.subsec_nanos()))?;
    Some(nanos)
}

fn nanos_to_delta(nanos: u128) -> Result<TimeDelta, SchedulerError> {
    let seconds = nanos / 1_000_000_000;
    let subsec_nanos = (nanos % 1_000_000_000) as u32;
    let seconds = i64::try_from(seconds).map_err(|_| SchedulerError::invalid_interval_out_of_range())?;
    TimeDelta::new(seconds, subsec_nanos).ok_or_else(SchedulerError::invalid_interval_out_of_range)
}

fn anchor_in_window(
    lower_bound: DateTime<Utc>,
    upper_bound: DateTime<Utc>,
    cron: &CronSchedule,
    timezone: Tz,
) -> Result<Option<DateTime<Utc>>, SchedulerError> {
    let search_start = lower_bound
        .checked_sub_signed(TimeDelta::nanoseconds(1))
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
    let candidate = cron.next_after(search_start, timezone);
    if let Some(anchor) = candidate
        && anchor > lower_bound
        && anchor <= upper_bound
    {
        return Ok(Some(anchor));
    }
    Ok(None)
}

pub(crate) fn stable_seed_hash(seed: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in seed.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn invalid_grouped_cron(message: impl Into<String>) -> SchedulerError {
    SchedulerError::invalid_job_with_kind(InvalidJobKind::Other, message)
}

#[cfg(test)]
#[path = "grouped_cron_tests.rs"]
mod tests;
