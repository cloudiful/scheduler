use crate::error::SchedulerError;
use crate::model::{Job, JobState, Schedule, utc_time};
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

    let candidate = match &job.schedule {
        Schedule::Interval(every) => duration_to_delta(*every)
            .ok_or_else(SchedulerError::invalid_interval_out_of_range)
            .and_then(|delta| {
                Utc::now()
                    .checked_add_signed(delta)
                    .ok_or_else(SchedulerError::invalid_interval_out_of_range)
            })
            .map(Some),
        Schedule::StaggeredInterval(staggered) => {
            super::interval_phase::staggered_initial_next_run_at(Utc::now(), staggered, &job.job_id)
                .map(Some)
        }
        Schedule::GroupedInterval(grouped) => {
            super::interval_phase::grouped_initial_next_run_at(Utc::now(), grouped).map(Some)
        }
        Schedule::WindowedInterval(windowed) => {
            super::windowed_interval::next_after(Utc::now(), windowed, timezone)
        }
        Schedule::AtTimes(times) => Ok(times.first().copied().map(utc_time)),
        Schedule::Cron(schedule) => Ok(schedule.next_after(Utc::now(), timezone)),
    }?;

    Ok(align_candidate_if_interval_schedule(
        job, candidate, timezone,
    ))
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

pub(crate) fn advance_state_for_manual_trigger<D>(job: &Job<D>, state: &mut JobState) -> u32 {
    state.trigger_count += 1;
    if let Some(max_runs) = job.max_runs
        && state.trigger_count >= max_runs
    {
        state.next_run_at = None;
    }
    state.trigger_count
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

    let candidate = match &job.schedule {
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
        Schedule::GroupedInterval(grouped) => {
            let delta = duration_to_delta(grouped.every)
                .ok_or_else(SchedulerError::invalid_interval_out_of_range)?;
            Ok(scheduled_at.checked_add_signed(delta))
        }
        Schedule::WindowedInterval(windowed) => {
            super::windowed_interval::next_after(scheduled_at, windowed, timezone)
        }
        Schedule::AtTimes(times) => Ok(times.get(trigger_count as usize).copied().map(utc_time)),
        Schedule::Cron(schedule) => Ok(schedule.next_after(scheduled_at, timezone)),
    }?;

    Ok(align_candidate_if_interval_schedule(
        job, candidate, timezone,
    ))
}

fn align_candidate_if_interval_schedule<D>(
    job: &Job<D>,
    candidate: Option<DateTime<Utc>>,
    timezone: Tz,
) -> Option<DateTime<Utc>> {
    if !matches!(
        job.schedule,
        Schedule::Interval(_)
            | Schedule::StaggeredInterval(_)
            | Schedule::GroupedInterval(_)
            | Schedule::WindowedInterval(_)
    ) {
        return candidate;
    }

    super::time_window_alignment::align_to_window(
        candidate,
        job.time_window_alignment,
        job.time_window.as_ref(),
        timezone,
    )
}

pub(crate) fn is_missed(scheduled_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    let tolerance = TimeDelta::milliseconds(25);
    scheduled_at
        .checked_add_signed(tolerance)
        .map(|adjusted| adjusted < now)
        .unwrap_or(false)
}

pub(super) fn duration_to_delta(duration: Duration) -> Option<TimeDelta> {
    TimeDelta::from_std(duration).ok()
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
#[path = "trigger_math_tests.rs"]
mod tests;
