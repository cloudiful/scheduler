use crate::error::SchedulerError;
use crate::model::{Job, JobState, Schedule, utc_time};
use chrono::{DateTime, TimeDelta, Utc};
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
) -> Result<Option<DateTime<Utc>>, SchedulerError> {
    if matches!(job.max_runs, Some(0)) {
        return Ok(None);
    }

    match &job.schedule {
        Schedule::Interval(every) => duration_to_delta(*every)
            .ok_or_else(|| {
                SchedulerError::invalid_job("interval schedule is too large to represent")
            })
            .and_then(|delta| {
                Utc::now().checked_add_signed(delta).ok_or_else(|| {
                    SchedulerError::invalid_job("interval schedule is too large to represent")
                })
            })
            .map(Some),
        Schedule::AtTimes(times) => Ok(times.first().copied().map(utc_time)),
    }
}

pub(crate) fn next_run_is_in_future(next_run_at: Option<DateTime<Utc>>) -> bool {
    next_run_at.map(|value| value > Utc::now()).unwrap_or(false)
}

pub(crate) fn inspect_due_window<D>(
    job: &Job<D>,
    state: &JobState,
    now: DateTime<Utc>,
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
    let has_multiple = compute_next_after(job, first_due, next_trigger_count)?
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
        advance_state_to(job, state, value)?;
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
) -> Result<(), SchedulerError>
where
    D: Send + Sync + 'static,
{
    state.trigger_count += 1;
    state.next_run_at = compute_next_after(job, scheduled_at, state.trigger_count)?;
    Ok(())
}

pub(crate) fn compute_next_after<D>(
    job: &Job<D>,
    scheduled_at: DateTime<Utc>,
    trigger_count: u32,
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
            let delta = duration_to_delta(*every).ok_or_else(|| {
                SchedulerError::invalid_job("interval schedule is too large to represent")
            })?;
            Ok(scheduled_at.checked_add_signed(delta))
        }
        Schedule::AtTimes(times) => Ok(times.get(trigger_count as usize).copied().map(utc_time)),
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

#[cfg(test)]
pub(crate) fn collect_due_times<D>(
    job: &Job<D>,
    state: &JobState,
    now: DateTime<Utc>,
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
        next_run_at = compute_next_after(job, value, trigger_count)?;
    }

    Ok(due_times)
}

#[cfg(test)]
pub(crate) fn advance_state_for<D>(
    job: &Job<D>,
    state: &mut JobState,
    due_times: &[DateTime<Utc>],
) -> Result<(), SchedulerError>
where
    D: Send + Sync + 'static,
{
    for scheduled_at in due_times {
        advance_state_to(job, state, *scheduled_at)?;
    }

    Ok(())
}
