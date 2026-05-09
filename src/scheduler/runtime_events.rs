use crate::execution_guard::ExecutionLease;
use crate::model::{JobState, RunRecord};
use crate::observer::{SchedulerEvent, StateLoadSource};

pub(super) fn state_loaded(
    job_id: &str,
    state: &JobState,
    source: StateLoadSource,
) -> SchedulerEvent {
    SchedulerEvent::StateLoaded {
        job_id: job_id.to_string(),
        trigger_count: state.trigger_count,
        next_run_at: state.next_run_at,
        source,
    }
}

pub(super) fn state_repaired(
    job_id: &str,
    state: &JobState,
    previous_next_run_at: Option<chrono::DateTime<chrono::Utc>>,
) -> SchedulerEvent {
    SchedulerEvent::StateRepaired {
        job_id: job_id.to_string(),
        trigger_count: state.trigger_count,
        previous_next_run_at,
        repaired_next_run_at: state.next_run_at,
    }
}

pub(super) fn trigger_emitted(
    job_id: &str,
    scheduled_at: chrono::DateTime<chrono::Utc>,
    catch_up: bool,
    trigger_count: u32,
) -> SchedulerEvent {
    SchedulerEvent::TriggerEmitted {
        job_id: job_id.to_string(),
        scheduled_at,
        catch_up,
        trigger_count,
    }
}

pub(super) fn execution_guard_acquired(
    lease: &ExecutionLease,
    catch_up: bool,
    trigger_count: u32,
) -> SchedulerEvent {
    SchedulerEvent::ExecutionGuardAcquired {
        job_id: lease.job_id.clone(),
        resource_id: lease.resource_id.clone(),
        scope: lease.scope,
        lease_key: lease.lease_key.clone(),
        scheduled_at: lease.scheduled_at,
        catch_up,
        trigger_count,
    }
}

pub(super) fn execution_guard_released(
    lease: &ExecutionLease,
    catch_up: bool,
    trigger_count: u32,
) -> SchedulerEvent {
    SchedulerEvent::ExecutionGuardReleased {
        job_id: lease.job_id.clone(),
        resource_id: lease.resource_id.clone(),
        scope: lease.scope,
        lease_key: lease.lease_key.clone(),
        scheduled_at: lease.scheduled_at,
        catch_up,
        trigger_count,
    }
}

pub(super) fn run_completed(
    job_id: &str,
    record: &RunRecord,
    trigger_count: u32,
) -> SchedulerEvent {
    SchedulerEvent::RunCompleted {
        job_id: job_id.to_string(),
        scheduled_at: record.scheduled_at,
        catch_up: record.catch_up,
        trigger_count,
        status: record.status.clone(),
        error: record.error.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{execution_guard_acquired, execution_guard_released, run_completed, state_loaded};
    use crate::execution_guard::{ExecutionGuardScope, ExecutionLease};
    use crate::model::{JobState, RunRecord, RunStatus};
    use crate::observer::{SchedulerEvent, StateLoadSource};
    use chrono::Utc;

    #[test]
    fn state_loaded_uses_runtime_state_fields() {
        let state = JobState::new("job", Some(Utc::now()));
        let event = state_loaded("job", &state, StateLoadSource::New);
        assert!(matches!(
            event,
            SchedulerEvent::StateLoaded { job_id, trigger_count, next_run_at, source }
                if job_id == "job"
                    && trigger_count == state.trigger_count
                    && next_run_at == state.next_run_at
                    && source == StateLoadSource::New
        ));
    }

    #[test]
    fn guard_events_preserve_lease_payload() {
        let lease = ExecutionLease::new(
            "job",
            "resource",
            ExecutionGuardScope::Occurrence,
            Some(Utc::now()),
            "token",
            "lease-key",
        );
        let acquired = execution_guard_acquired(&lease, true, 7);
        let released = execution_guard_released(&lease, true, 7);
        assert!(matches!(
            acquired,
            SchedulerEvent::ExecutionGuardAcquired { lease_key, catch_up, trigger_count, .. }
                if lease_key == "lease-key" && catch_up && trigger_count == 7
        ));
        assert!(matches!(
            released,
            SchedulerEvent::ExecutionGuardReleased { lease_key, catch_up, trigger_count, .. }
                if lease_key == "lease-key" && catch_up && trigger_count == 7
        ));
    }

    #[test]
    fn run_completed_uses_record_fields() {
        let scheduled_at = Utc::now();
        let record = RunRecord {
            scheduled_at,
            started_at: scheduled_at,
            finished_at: scheduled_at,
            catch_up: false,
            status: RunStatus::Success,
            error: None,
        };
        let event = run_completed("job", &record, 3);
        assert!(matches!(
            event,
            SchedulerEvent::RunCompleted { job_id, scheduled_at: event_at, trigger_count, .. }
                if job_id == "job" && event_at == scheduled_at && trigger_count == 3
        ));
    }
}
