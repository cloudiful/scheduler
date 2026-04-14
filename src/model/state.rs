use chrono::{DateTime, Utc};
#[cfg(feature = "valkey-store")]
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    pub scheduled_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub catch_up: bool,
    pub status: RunStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "valkey-store", derive(Serialize, Deserialize))]
pub struct JobState {
    pub job_id: String,
    pub trigger_count: u32,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

impl JobState {
    pub fn new(job_id: impl Into<String>, next_run_at: Option<DateTime<Utc>>) -> Self {
        Self {
            job_id: job_id.into(),
            trigger_count: 0,
            last_run_at: None,
            last_success_at: None,
            next_run_at,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerReport {
    pub job_id: String,
    pub state: JobState,
    pub history: Vec<RunRecord>,
}

pub(crate) fn push_history(
    history: &mut VecDeque<RunRecord>,
    record: RunRecord,
    history_limit: usize,
) {
    if history_limit == 0 {
        return;
    }

    history.push_back(record);
    while history.len() > history_limit {
        history.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::{RunRecord, RunStatus, push_history};
    use chrono::Utc;
    use std::collections::VecDeque;

    fn record(second: i64) -> RunRecord {
        let instant = Utc::now() + chrono::TimeDelta::seconds(second);
        RunRecord {
            scheduled_at: instant,
            started_at: instant,
            finished_at: instant,
            catch_up: false,
            status: RunStatus::Success,
            error: None,
        }
    }

    #[test]
    fn push_history_keeps_latest_records_within_limit() {
        let base = Utc::now();
        let mut history = VecDeque::new();
        push_history(&mut history, record_at(base, 1), 2);
        push_history(&mut history, record_at(base, 2), 2);
        push_history(&mut history, record_at(base, 3), 2);

        assert_eq!(history.len(), 2);
        assert_eq!(
            history.front().unwrap().scheduled_at,
            record_at(base, 2).scheduled_at
        );
        assert_eq!(
            history.back().unwrap().scheduled_at,
            record_at(base, 3).scheduled_at
        );
    }

    #[test]
    fn push_history_ignores_records_when_limit_is_zero() {
        let mut history = VecDeque::new();
        push_history(&mut history, record(1), 0);

        assert!(history.is_empty());
    }

    fn record_at(base: chrono::DateTime<Utc>, second: i64) -> RunRecord {
        let instant = base + chrono::TimeDelta::seconds(second);
        RunRecord {
            scheduled_at: instant,
            started_at: instant,
            finished_at: instant,
            catch_up: false,
            status: RunStatus::Success,
            error: None,
        }
    }
}
