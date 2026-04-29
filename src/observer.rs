use crate::model::RunStatus;
use crate::store::StoreOperation;
use chrono::{DateTime, Utc};
use log::{debug, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateLoadSource {
    New,
    Restored,
    Repaired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerStopReason {
    Cancelled,
    Shutdown,
    Terminal,
    ChannelClosed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerEvent {
    StateLoaded {
        job_id: String,
        trigger_count: u32,
        next_run_at: Option<DateTime<Utc>>,
        source: StateLoadSource,
    },
    StateRepaired {
        job_id: String,
        trigger_count: u32,
        previous_next_run_at: Option<DateTime<Utc>>,
        repaired_next_run_at: Option<DateTime<Utc>>,
    },
    TriggerEmitted {
        job_id: String,
        scheduled_at: DateTime<Utc>,
        catch_up: bool,
        trigger_count: u32,
    },
    RunCompleted {
        job_id: String,
        scheduled_at: DateTime<Utc>,
        catch_up: bool,
        trigger_count: u32,
        status: RunStatus,
        error: Option<String>,
    },
    ExecutionGuardContended {
        job_id: String,
        scheduled_at: DateTime<Utc>,
        catch_up: bool,
        trigger_count: u32,
    },
    ExecutionGuardLost {
        job_id: String,
        scheduled_at: DateTime<Utc>,
        catch_up: bool,
        trigger_count: u32,
    },
    ExecutionGuardReleaseFailed {
        job_id: String,
        scheduled_at: DateTime<Utc>,
        catch_up: bool,
        trigger_count: u32,
        error: String,
    },
    StoreDegraded {
        job_id: String,
        operation: StoreOperation,
        error: String,
    },
    TerminalStateDeleted {
        job_id: String,
        trigger_count: u32,
    },
    SchedulerStopped {
        job_id: String,
        trigger_count: u32,
        reason: SchedulerStopReason,
    },
}

pub trait SchedulerObserver: Send + Sync + 'static {
    fn on_event(&self, event: &SchedulerEvent);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopObserver;

impl SchedulerObserver for NoopObserver {
    fn on_event(&self, _event: &SchedulerEvent) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LogObserver;

impl SchedulerObserver for LogObserver {
    fn on_event(&self, event: &SchedulerEvent) {
        match event {
            SchedulerEvent::StateLoaded {
                job_id,
                trigger_count,
                next_run_at,
                source,
            } => info!(
                "scheduler state loaded job_id={} source={:?} trigger_count={} next_run_at={:?}",
                job_id, source, trigger_count, next_run_at
            ),
            SchedulerEvent::StateRepaired {
                job_id,
                trigger_count,
                previous_next_run_at,
                repaired_next_run_at,
            } => warn!(
                "scheduler repaired state job_id={} trigger_count={} previous_next_run_at={:?} repaired_next_run_at={:?}",
                job_id, trigger_count, previous_next_run_at, repaired_next_run_at
            ),
            SchedulerEvent::TriggerEmitted {
                job_id,
                scheduled_at,
                catch_up,
                trigger_count,
            } => debug!(
                "scheduler trigger emitted job_id={} scheduled_at={} catch_up={} trigger_count={}",
                job_id, scheduled_at, catch_up, trigger_count
            ),
            SchedulerEvent::RunCompleted {
                job_id,
                scheduled_at,
                catch_up,
                trigger_count,
                status,
                error,
            } => debug!(
                "scheduler run completed job_id={} scheduled_at={} catch_up={} trigger_count={} status={:?} error={:?}",
                job_id, scheduled_at, catch_up, trigger_count, status, error
            ),
            SchedulerEvent::ExecutionGuardContended {
                job_id,
                scheduled_at,
                catch_up,
                trigger_count,
            } => debug!(
                "scheduler execution guard contended job_id={} scheduled_at={} catch_up={} trigger_count={}",
                job_id, scheduled_at, catch_up, trigger_count
            ),
            SchedulerEvent::ExecutionGuardLost {
                job_id,
                scheduled_at,
                catch_up,
                trigger_count,
            } => warn!(
                "scheduler execution guard lost job_id={} scheduled_at={} catch_up={} trigger_count={}",
                job_id, scheduled_at, catch_up, trigger_count
            ),
            SchedulerEvent::ExecutionGuardReleaseFailed {
                job_id,
                scheduled_at,
                catch_up,
                trigger_count,
                error,
            } => warn!(
                "scheduler execution guard release failed job_id={} scheduled_at={} catch_up={} trigger_count={} error={}",
                job_id, scheduled_at, catch_up, trigger_count, error
            ),
            SchedulerEvent::StoreDegraded {
                job_id,
                operation,
                error,
            } => warn!(
                "scheduler store degraded job_id={} operation={:?} error={}",
                job_id, operation, error
            ),
            SchedulerEvent::TerminalStateDeleted {
                job_id,
                trigger_count,
            } => info!(
                "scheduler deleted terminal state job_id={} trigger_count={}",
                job_id, trigger_count
            ),
            SchedulerEvent::SchedulerStopped {
                job_id,
                trigger_count,
                reason,
            } => info!(
                "scheduler stopped job_id={} trigger_count={} reason={:?}",
                job_id, trigger_count, reason
            ),
        }
    }
}
