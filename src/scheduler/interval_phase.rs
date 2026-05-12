use crate::{GroupedIntervalSchedule, SchedulerError, StaggeredIntervalSchedule};
use chrono::{DateTime, Utc};
use std::time::Duration;

pub(crate) fn staggered_initial_next_run_at(
    now: DateTime<Utc>,
    staggered: &StaggeredIntervalSchedule,
    seed: &str,
) -> Result<DateTime<Utc>, SchedulerError> {
    let interval_nanos = interval_nanos(staggered.every)?;
    let phase_nanos =
        u128::from(stable_seed_hash(staggered.seed.as_deref().unwrap_or(seed))) % interval_nanos;

    aligned_initial_next_run_at(now, interval_nanos, phase_nanos)
}

pub(crate) fn grouped_initial_next_run_at(
    now: DateTime<Utc>,
    grouped: &GroupedIntervalSchedule,
) -> Result<DateTime<Utc>, SchedulerError> {
    validate_grouped_interval(grouped)?;
    let interval_nanos = interval_nanos(grouped.every)?;
    let base_phase_nanos = interval_nanos
        .checked_mul(u128::from(grouped.member_index))
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)?
        / u128::from(grouped.group_size);
    let group_offset_nanos = grouped
        .group_seed
        .as_deref()
        .map(|seed| u128::from(stable_seed_hash(seed)) % interval_nanos)
        .unwrap_or(0);
    let phase_nanos = base_phase_nanos
        .checked_add(group_offset_nanos)
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)?
        % interval_nanos;

    aligned_initial_next_run_at(now, interval_nanos, phase_nanos)
}

pub(crate) fn validate_grouped_interval(
    grouped: &GroupedIntervalSchedule,
) -> Result<(), SchedulerError> {
    if grouped.group_size == 0 {
        return Err(SchedulerError::invalid_job_with_kind(
            crate::InvalidJobKind::Other,
            "grouped interval group_size must be greater than zero",
        ));
    }
    if grouped.member_index >= grouped.group_size {
        return Err(SchedulerError::invalid_job_with_kind(
            crate::InvalidJobKind::Other,
            "grouped interval member_index must be less than group_size",
        ));
    }
    Ok(())
}

fn duration_to_nanos(duration: Duration) -> Option<u128> {
    let nanos = u128::from(duration.as_secs())
        .checked_mul(1_000_000_000)?
        .checked_add(u128::from(duration.subsec_nanos()))?;
    Some(nanos)
}

fn utc_to_nanos(value: DateTime<Utc>) -> i128 {
    i128::from(value.timestamp())
        .saturating_mul(1_000_000_000)
        .saturating_add(i128::from(value.timestamp_subsec_nanos()))
}

fn nanos_to_utc(value: i128) -> Option<DateTime<Utc>> {
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    let seconds = i64::try_from(seconds).ok()?;
    chrono::TimeZone::timestamp_opt(&Utc, seconds, nanos).single()
}

fn stable_seed_hash(seed: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in seed.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn interval_nanos(every: Duration) -> Result<u128, SchedulerError> {
    let interval_nanos =
        duration_to_nanos(every).ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
    if interval_nanos == 0 {
        return Err(SchedulerError::invalid_zero_interval());
    }
    Ok(interval_nanos)
}

fn aligned_initial_next_run_at(
    now: DateTime<Utc>,
    interval_nanos: u128,
    phase_nanos: u128,
) -> Result<DateTime<Utc>, SchedulerError> {
    let interval_nanos = i128::try_from(interval_nanos)
        .map_err(|_| SchedulerError::invalid_interval_out_of_range())?;
    let phase_nanos =
        i128::try_from(phase_nanos).map_err(|_| SchedulerError::invalid_interval_out_of_range())?;
    let now_nanos = utc_to_nanos(now);
    let cycle_start = now_nanos.div_euclid(interval_nanos) * interval_nanos;
    let mut candidate = cycle_start + phase_nanos;
    if candidate < now_nanos {
        candidate += interval_nanos;
    }

    nanos_to_utc(candidate).ok_or_else(SchedulerError::invalid_interval_out_of_range)
}

#[cfg(test)]
#[path = "interval_phase_tests.rs"]
mod tests;
