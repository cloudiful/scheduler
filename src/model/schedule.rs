use super::CronSchedule;
use chrono::{DateTime, Utc};
use chrono_tz::{Asia::Shanghai, Tz};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// Trigger repeatedly after the given interval.
    Interval(Duration),
    /// Trigger at the listed wall-clock times.
    ///
    /// The list is sorted before execution starts. An empty list is treated as
    /// a no-op schedule and exits without running.
    AtTimes(Vec<DateTime<Tz>>),
    /// Trigger using a standard 5-field cron expression.
    ///
    /// The scheduler timezone is used to calculate the next matching wall-clock
    /// time. The expression itself does not carry timezone information.
    Cron(CronSchedule),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MissedRunPolicy {
    Skip,
    #[default]
    CatchUpOnce,
    ReplayAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverlapPolicy {
    #[default]
    Forbid,
    QueueOne,
    AllowParallel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TerminalStatePolicy {
    #[default]
    Retain,
    Delete,
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// The timezone forwarded to each [`crate::RunContext`] and used to
    /// evaluate [`Schedule::Cron`] expressions.
    ///
    /// This does not rewrite [`Schedule::AtTimes`] values, which already carry
    /// their own timezone-aware timestamps.
    pub timezone: Tz,
    /// The maximum number of [`crate::RunRecord`] items kept in memory.
    pub history_limit: usize,
    /// Controls how persisted state is handled once a job reaches a terminal
    /// state (`next_run_at == None` and no further work is pending).
    pub terminal_state_policy: TerminalStatePolicy,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            timezone: Shanghai,
            history_limit: 32,
            terminal_state_policy: TerminalStatePolicy::Retain,
        }
    }
}

pub(crate) fn utc_time(value: DateTime<Tz>) -> DateTime<Utc> {
    value.with_timezone(&Utc)
}
