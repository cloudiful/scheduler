use crate::error::SchedulerError;
use crate::model::{Job, JobState, Schedule, utc_time};
use chrono::{DateTime, TimeDelta, Utc};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingTrigger {
    pub(crate) scheduled_at: DateTime<Utc>,
    pub(crate) catch_up: bool,
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

pub(crate) fn next_trigger<D>(
    job: &Job<D>,
    state: &mut JobState,
    now: DateTime<Utc>,
) -> Result<Option<PendingTrigger>, SchedulerError>
where
    D: Send + Sync + 'static,
{
    let Some(next_run_at) = state.next_run_at else {
        return Ok(None);
    };

    if next_run_at > now {
        return Ok(None);
    }

    let due_times = collect_due_times(job, state, now)?;
    if due_times.is_empty() {
        return Ok(None);
    }

    let first_due = due_times[0];
    if due_times.len() == 1 && !is_missed(first_due, now) {
        advance_state_to(job, state, first_due)?;
        return Ok(Some(PendingTrigger {
            scheduled_at: first_due,
            catch_up: false,
        }));
    }

    match job.missed_run_policy {
        crate::MissedRunPolicy::Skip => {
            advance_state_for(job, state, &due_times)?;
            Ok(None)
        }
        crate::MissedRunPolicy::CatchUpOnce => {
            let last_due = *due_times.last().expect("due_times is not empty");
            advance_state_for(job, state, &due_times)?;
            Ok(Some(PendingTrigger {
                scheduled_at: last_due,
                catch_up: due_times.len() > 1 || is_missed(last_due, now),
            }))
        }
        crate::MissedRunPolicy::ReplayAll => {
            advance_state_to(job, state, first_due)?;
            Ok(Some(PendingTrigger {
                scheduled_at: first_due,
                catch_up: due_times.len() > 1 || is_missed(first_due, now),
            }))
        }
    }
}

pub(crate) fn next_run_is_in_future(next_run_at: Option<DateTime<Utc>>) -> bool {
    next_run_at.map(|value| value > Utc::now()).unwrap_or(false)
}

fn collect_due_times<D>(
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

fn advance_state_to<D>(
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

fn advance_state_for<D>(
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

fn compute_next_after<D>(
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

fn duration_to_delta(duration: Duration) -> Option<TimeDelta> {
    TimeDelta::from_std(duration).ok()
}

fn is_missed(scheduled_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    let tolerance = TimeDelta::milliseconds(25);
    scheduled_at
        .checked_add_signed(tolerance)
        .map(|adjusted| adjusted < now)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        PendingTrigger, advance_state_for, collect_due_times, compute_next_after, next_trigger,
    };
    use crate::{Job, JobState, MissedRunPolicy, Schedule, Task};
    use chrono::{TimeDelta, Utc};
    use chrono_tz::Asia::Shanghai;
    use std::time::Duration;

    fn noop_job(schedule: Schedule) -> Job<()> {
        Job::without_deps("job", schedule, Task::from_async(|_| async { Ok(()) }))
    }

    #[test]
    fn compute_next_after_stops_at_max_runs_for_interval() {
        let scheduled_at = Utc::now();
        let job = noop_job(Schedule::Interval(Duration::from_secs(5))).with_max_runs(2);

        let next = compute_next_after(&job, scheduled_at, 2).unwrap();

        assert!(next.is_none());
    }

    #[test]
    fn collect_due_times_replays_all_past_at_times() {
        let now = Utc::now();
        let times = vec![
            (now - TimeDelta::seconds(2)).with_timezone(&Shanghai),
            (now - TimeDelta::seconds(1)).with_timezone(&Shanghai),
            (now + TimeDelta::seconds(3)).with_timezone(&Shanghai),
        ];
        let state = JobState::new("job", Some(times[0].with_timezone(&Utc)));
        let job = noop_job(Schedule::AtTimes(times));

        let due = collect_due_times(&job, &state, now).unwrap();

        assert_eq!(due.len(), 2);
    }

    #[test]
    fn next_trigger_skip_advances_state_without_emitting_run() {
        let now = Utc::now();
        let times = vec![
            (now - TimeDelta::seconds(2)).with_timezone(&Shanghai),
            (now + TimeDelta::seconds(2)).with_timezone(&Shanghai),
        ];
        let mut state = JobState::new("job", Some(times[0].with_timezone(&Utc)));
        let job = noop_job(Schedule::AtTimes(times)).with_missed_run_policy(MissedRunPolicy::Skip);

        let trigger = next_trigger(&job, &mut state, now).unwrap();

        assert!(trigger.is_none());
        assert_eq!(state.trigger_count, 1);
        assert!(state.next_run_at.unwrap() > now);
    }

    #[test]
    fn next_trigger_replay_all_returns_oldest_due_run() {
        let now = Utc::now();
        let first = (now - TimeDelta::seconds(2)).with_timezone(&Shanghai);
        let second = (now - TimeDelta::seconds(1)).with_timezone(&Shanghai);
        let mut state = JobState::new("job", Some(first.with_timezone(&Utc)));
        let job = noop_job(Schedule::AtTimes(vec![first, second]))
            .with_missed_run_policy(MissedRunPolicy::ReplayAll);

        let trigger = next_trigger(&job, &mut state, now).unwrap();

        assert_eq!(
            trigger,
            Some(PendingTrigger {
                scheduled_at: first.with_timezone(&Utc),
                catch_up: true,
            })
        );
        assert_eq!(state.trigger_count, 1);
    }

    #[test]
    fn advance_state_for_consumes_multiple_due_times() {
        let now = Utc::now();
        let first = (now - TimeDelta::seconds(3)).with_timezone(&Shanghai);
        let second = (now - TimeDelta::seconds(2)).with_timezone(&Shanghai);
        let third = (now + TimeDelta::seconds(1)).with_timezone(&Shanghai);
        let mut state = JobState::new("job", Some(first.with_timezone(&Utc)));
        let job = noop_job(Schedule::AtTimes(vec![first, second, third]));

        advance_state_for(
            &job,
            &mut state,
            &[first.with_timezone(&Utc), second.with_timezone(&Utc)],
        )
        .unwrap();

        assert_eq!(state.trigger_count, 2);
        assert_eq!(state.next_run_at, Some(third.with_timezone(&Utc)));
    }
}
