use crate::error::SchedulerError;
use crate::model::{Job, JobState, Schedule, StaggeredIntervalSchedule, utc_time};
use chrono::{DateTime, TimeDelta, Utc};
use chrono_tz::Tz;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DueSummary {
    pub(crate) first_due: DateTime<Utc>,
    pub(crate) last_due: DateTime<Utc>,
    pub(crate) count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DueWindow {
    pub(crate) first_due: DateTime<Utc>,
    pub(crate) has_multiple: bool,
}

pub(crate) fn initial_next_run_at<D>(
    job: &Job<D>,
    timezone: Tz,
) -> Result<Option<DateTime<Utc>>, SchedulerError> {
    if matches!(job.max_runs, Some(0)) {
        return Ok(None);
    }

    match &job.schedule {
        Schedule::Interval(every) => duration_to_delta(*every)
            .ok_or_else(SchedulerError::invalid_interval_out_of_range)
            .and_then(|delta| {
                Utc::now()
                    .checked_add_signed(delta)
                    .ok_or_else(SchedulerError::invalid_interval_out_of_range)
            })
            .map(Some),
        Schedule::StaggeredInterval(staggered) => {
            staggered_initial_next_run_at(Utc::now(), staggered, &job.job_id).map(Some)
        }
        Schedule::AtTimes(times) => Ok(times.first().copied().map(utc_time)),
        Schedule::Cron(schedule) => Ok(schedule.next_after(Utc::now(), timezone)),
    }
}

pub(crate) fn next_run_is_in_future(next_run_at: Option<DateTime<Utc>>) -> bool {
    next_run_at.map(|value| value > Utc::now()).unwrap_or(false)
}

pub(crate) fn inspect_due_window<D>(
    job: &Job<D>,
    state: &JobState,
    now: DateTime<Utc>,
    timezone: Tz,
) -> Result<Option<DueWindow>, SchedulerError>
where
    D: Send + Sync + 'static,
{
    let Some(first_due) = state.next_run_at else {
        return Ok(None);
    };

    if first_due > now {
        return Ok(None);
    }

    let next_trigger_count = state.trigger_count + 1;
    let has_multiple = compute_next_after(job, first_due, next_trigger_count, timezone)?
        .map(|next_due| next_due <= now)
        .unwrap_or(false);

    Ok(Some(DueWindow {
        first_due,
        has_multiple,
    }))
}

pub(crate) fn consume_due_summary<D>(
    job: &Job<D>,
    state: &mut JobState,
    now: DateTime<Utc>,
    timezone: Tz,
) -> Result<Option<DueSummary>, SchedulerError>
where
    D: Send + Sync + 'static,
{
    let mut first_due = None;
    let mut last_due = None;
    let mut count = 0u32;

    while let Some(value) = state.next_run_at {
        if value > now {
            break;
        }

        if first_due.is_none() {
            first_due = Some(value);
        }
        last_due = Some(value);
        count += 1;
        advance_state_to(job, state, value, timezone)?;
    }

    Ok(first_due.map(|first_due| DueSummary {
        first_due,
        last_due: last_due.expect("last_due exists when first_due exists"),
        count,
    }))
}

pub(crate) fn advance_state_to<D>(
    job: &Job<D>,
    state: &mut JobState,
    scheduled_at: DateTime<Utc>,
    timezone: Tz,
) -> Result<(), SchedulerError>
where
    D: Send + Sync + 'static,
{
    state.trigger_count += 1;
    state.next_run_at = compute_next_after(job, scheduled_at, state.trigger_count, timezone)?;
    Ok(())
}

pub(crate) fn compute_next_after<D>(
    job: &Job<D>,
    scheduled_at: DateTime<Utc>,
    trigger_count: u32,
    timezone: Tz,
) -> Result<Option<DateTime<Utc>>, SchedulerError>
where
    D: Send + Sync + 'static,
{
    if let Some(max_runs) = job.max_runs
        && trigger_count >= max_runs
    {
        return Ok(None);
    }

    match &job.schedule {
        Schedule::Interval(every) => {
            let delta = duration_to_delta(*every)
                .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
            Ok(scheduled_at.checked_add_signed(delta))
        }
        Schedule::StaggeredInterval(staggered) => {
            let delta = duration_to_delta(staggered.every)
                .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
            Ok(scheduled_at.checked_add_signed(delta))
        }
        Schedule::AtTimes(times) => Ok(times.get(trigger_count as usize).copied().map(utc_time)),
        Schedule::Cron(schedule) => Ok(schedule.next_after(scheduled_at, timezone)),
    }
}

pub(crate) fn is_missed(scheduled_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    let tolerance = TimeDelta::milliseconds(25);
    scheduled_at
        .checked_add_signed(tolerance)
        .map(|adjusted| adjusted < now)
        .unwrap_or(false)
}

fn duration_to_delta(duration: Duration) -> Option<TimeDelta> {
    TimeDelta::from_std(duration).ok()
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

fn staggered_initial_next_run_at(
    now: DateTime<Utc>,
    staggered: &StaggeredIntervalSchedule,
    seed: &str,
) -> Result<DateTime<Utc>, SchedulerError> {
    let interval_nanos = duration_to_nanos(staggered.every)
        .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
    if interval_nanos == 0 {
        return Err(SchedulerError::invalid_zero_interval());
    }

    let interval_nanos = i128::try_from(interval_nanos)
        .map_err(|_| SchedulerError::invalid_interval_out_of_range())?;
    let phase_nanos = i128::from(stable_seed_hash(staggered.seed.as_deref().unwrap_or(seed)))
        .rem_euclid(interval_nanos);
    let now_nanos = utc_to_nanos(now);
    let cycle_start = now_nanos.div_euclid(interval_nanos) * interval_nanos;
    let mut candidate = cycle_start + phase_nanos;
    if candidate < now_nanos {
        candidate += interval_nanos;
    }

    nanos_to_utc(candidate).ok_or_else(SchedulerError::invalid_interval_out_of_range)
}

#[cfg(test)]
pub(crate) fn collect_due_times<D>(
    job: &Job<D>,
    state: &JobState,
    now: DateTime<Utc>,
    timezone: Tz,
) -> Result<Vec<DateTime<Utc>>, SchedulerError>
where
    D: Send + Sync + 'static,
{
    let mut due_times = Vec::new();
    let mut trigger_count = state.trigger_count;
    let mut next_run_at = state.next_run_at;

    while let Some(value) = next_run_at {
        if value > now {
            break;
        }

        due_times.push(value);
        trigger_count += 1;
        next_run_at = compute_next_after(job, value, trigger_count, timezone)?;
    }

    Ok(due_times)
}

#[cfg(test)]
pub(crate) fn advance_state_for<D>(
    job: &Job<D>,
    state: &mut JobState,
    due_times: &[DateTime<Utc>],
    timezone: Tz,
) -> Result<(), SchedulerError>
where
    D: Send + Sync + 'static,
{
    for scheduled_at in due_times {
        advance_state_to(job, state, *scheduled_at, timezone)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        advance_state_for, collect_due_times, compute_next_after, nanos_to_utc, stable_seed_hash,
        staggered_initial_next_run_at, utc_to_nanos,
    };
    use crate::{Job, JobState, MissedRunPolicy, Schedule, StaggeredIntervalSchedule, Task};
    use chrono::{TimeDelta, TimeZone, Utc};
    use chrono_tz::Asia::Shanghai;
    use std::time::Duration;

    fn noop_job(schedule: Schedule) -> Job<()> {
        Job::without_deps("job", schedule, Task::from_async(|_| async { Ok(()) }))
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
            staggered_initial_next_run_at(now, &schedule.clone().with_seed("site-a"), "job-a")
                .unwrap(),
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
}
