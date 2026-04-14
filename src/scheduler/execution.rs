use crate::model::{
    JobState, RunContext, RunRecord, RunStatus, TaskContext, TaskHandler, push_history,
};
use crate::scheduler::trigger::PendingTrigger;
use chrono::Utc;
use chrono_tz::Tz;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::task::JoinSet;

#[derive(Debug)]
pub(crate) struct CompletedRun {
    pub(crate) record: RunRecord,
}

impl CompletedRun {
    pub(crate) fn apply_to(
        self,
        state: &mut JobState,
        history: &mut VecDeque<RunRecord>,
        history_limit: usize,
    ) -> RunRecord {
        state.last_run_at = Some(self.record.started_at);
        match self.record.status {
            RunStatus::Success => {
                state.last_success_at = Some(self.record.finished_at);
                state.last_error = None;
            }
            RunStatus::Failed => {
                state.last_error = self.record.error.clone();
            }
        }

        push_history(history, self.record.clone(), history_limit);
        self.record
    }
}

pub(crate) fn spawn_trigger<D>(
    active: &mut JoinSet<CompletedRun>,
    task: TaskHandler<D>,
    deps: Arc<D>,
    job_id: String,
    timezone: Tz,
    trigger: PendingTrigger,
) where
    D: Send + Sync + 'static,
{
    active.spawn(async move {
        let started_at = Utc::now();
        let result = task(TaskContext {
            run: RunContext {
                job_id,
                scheduled_at: trigger.scheduled_at,
                catch_up: trigger.catch_up,
                timezone,
            },
            deps,
        })
        .await;
        let finished_at = Utc::now();

        let (status, error) = match result {
            Ok(()) => (RunStatus::Success, None),
            Err(message) => (RunStatus::Failed, Some(message)),
        };

        CompletedRun {
            record: RunRecord {
                scheduled_at: trigger.scheduled_at,
                started_at,
                finished_at,
                catch_up: trigger.catch_up,
                status,
                error,
            },
        }
    });
}
