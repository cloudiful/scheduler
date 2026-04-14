use crate::error::SchedulerError;
use crate::model::{Job, JobState};
use crate::scheduler::trigger_math::{
    advance_state_to, consume_due_summary, inspect_due_window, is_missed,
};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingTrigger {
    pub(crate) scheduled_at: DateTime<Utc>,
    pub(crate) catch_up: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerDecision {
    Idle,
    StateAdvanced,
    Trigger(PendingTrigger),
}

pub(crate) fn next_trigger<D>(
    job: &Job<D>,
    state: &mut JobState,
    now: DateTime<Utc>,
    timezone: Tz,
) -> Result<TriggerDecision, SchedulerError>
where
    D: Send + Sync + 'static,
{
    let Some(next_run_at) = state.next_run_at else {
        return Ok(TriggerDecision::Idle);
    };

    if next_run_at > now {
        return Ok(TriggerDecision::Idle);
    }

    match job.missed_run_policy {
        crate::MissedRunPolicy::Skip => {
            let Some(summary) = consume_due_summary(job, state, now, timezone)? else {
                return Ok(TriggerDecision::Idle);
            };

            if summary.count == 1 && !is_missed(summary.first_due, now) {
                return Ok(TriggerDecision::Trigger(PendingTrigger {
                    scheduled_at: summary.first_due,
                    catch_up: false,
                }));
            }

            Ok(TriggerDecision::StateAdvanced)
        }
        crate::MissedRunPolicy::CatchUpOnce => {
            let Some(summary) = consume_due_summary(job, state, now, timezone)? else {
                return Ok(TriggerDecision::Idle);
            };

            let scheduled_at = if summary.count == 1 {
                summary.first_due
            } else {
                summary.last_due
            };

            Ok(TriggerDecision::Trigger(PendingTrigger {
                scheduled_at,
                catch_up: summary.count > 1 || is_missed(scheduled_at, now),
            }))
        }
        crate::MissedRunPolicy::ReplayAll => {
            let Some(window) = inspect_due_window(job, state, now, timezone)? else {
                return Ok(TriggerDecision::Idle);
            };

            let first_due = window.first_due;
            advance_state_to(job, state, first_due, timezone)?;
            Ok(TriggerDecision::Trigger(PendingTrigger {
                scheduled_at: first_due,
                catch_up: window.has_multiple || is_missed(first_due, now),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingTrigger, TriggerDecision, next_trigger};
    use crate::scheduler::trigger_math::{
        advance_state_for, collect_due_times, compute_next_after, consume_due_summary,
        inspect_due_window,
    };
    use crate::{CronSchedule, Job, JobState, MissedRunPolicy, Schedule, Task};
    use chrono::{TimeDelta, TimeZone, Utc};
    use chrono_tz::Asia::Shanghai;
    use std::time::Duration;

    fn noop_job(schedule: Schedule) -> Job<()> {
        Job::without_deps("job", schedule, Task::from_async(|_| async { Ok(()) }))
    }

    #[test]
    fn compute_next_after_stops_at_max_runs_for_interval() {
        let scheduled_at = Utc::now();
        let job = noop_job(Schedule::Interval(Duration::from_secs(5))).with_max_runs(2);

        let next = compute_next_after(&job, scheduled_at, 2, Shanghai).unwrap();

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

        let due = collect_due_times(&job, &state, now, Shanghai).unwrap();

        assert_eq!(due.len(), 2);
    }

    #[test]
    fn inspect_due_window_detects_multiple_due_runs() {
        let now = Utc::now();
        let state = JobState::new("job", Some(now - TimeDelta::seconds(2)));
        let job = noop_job(Schedule::Interval(Duration::from_secs(1))).with_max_runs(4);

        let window = inspect_due_window(&job, &state, now, Shanghai)
            .unwrap()
            .unwrap();

        assert_eq!(window.first_due, now - TimeDelta::seconds(2));
        assert!(window.has_multiple);
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

        let trigger = next_trigger(&job, &mut state, now, Shanghai).unwrap();

        assert_eq!(trigger, TriggerDecision::StateAdvanced);
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

        let trigger = next_trigger(&job, &mut state, now, Shanghai).unwrap();

        assert_eq!(
            trigger,
            TriggerDecision::Trigger(PendingTrigger {
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
            Shanghai,
        )
        .unwrap();

        assert_eq!(state.trigger_count, 2);
        assert_eq!(state.next_run_at, Some(third.with_timezone(&Utc)));
    }

    #[test]
    fn consume_due_summary_advances_without_collecting_every_due_time() {
        let now = Utc::now();
        let mut state = JobState::new("job", Some(now - TimeDelta::seconds(3)));
        let job = noop_job(Schedule::Interval(Duration::from_secs(1)));

        let summary = consume_due_summary(&job, &mut state, now, Shanghai)
            .unwrap()
            .unwrap();

        assert_eq!(summary.first_due, now - TimeDelta::seconds(3));
        assert_eq!(summary.last_due, now);
        assert_eq!(summary.count, 4);
        assert_eq!(state.trigger_count, 4);
        assert_eq!(state.next_run_at, Some(now + TimeDelta::seconds(1)));
    }

    #[test]
    fn compute_next_after_advances_cron_from_scheduled_time() {
        let scheduled_at = Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 0).unwrap();
        let job = noop_job(Schedule::Cron(CronSchedule::parse("* * * * *").unwrap()));

        let next = compute_next_after(&job, scheduled_at, 1, chrono_tz::UTC).unwrap();

        assert_eq!(
            next,
            Some(Utc.with_ymd_and_hms(2026, 4, 3, 1, 3, 0).unwrap())
        );
    }
}
