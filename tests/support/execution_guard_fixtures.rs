use scheduler::{
    ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardErrorKind, ExecutionGuardRenewal,
    ExecutionLease, ExecutionSlot, SchedulerEvent, SchedulerObserver,
};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Default)]
pub struct RecordingObserver {
    events: Arc<Mutex<Vec<SchedulerEvent>>>,
}

impl RecordingObserver {
    pub fn snapshot(&self) -> Vec<SchedulerEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl SchedulerObserver for RecordingObserver {
    fn on_event(&self, event: &SchedulerEvent) {
        self.events.lock().unwrap().push(event.clone());
    }
}

#[derive(Debug, Clone)]
pub struct FakeGuardError {
    kind: ExecutionGuardErrorKind,
    message: &'static str,
}

impl FakeGuardError {
    pub fn kind(&self) -> ExecutionGuardErrorKind {
        self.kind
    }
}

impl Display for FakeGuardError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}

impl Error for FakeGuardError {}

#[derive(Debug, Clone, Copy)]
pub enum AcquirePlan {
    Acquired,
    Contended,
    Error(ExecutionGuardErrorKind, &'static str),
}

#[derive(Debug, Clone, Copy)]
pub enum RenewPlan {
    Renewed,
    Lost,
    Error(ExecutionGuardErrorKind, &'static str),
}

#[derive(Debug, Clone, Copy)]
pub enum ReleasePlan {
    Ok,
    Error(ExecutionGuardErrorKind, &'static str),
}

#[derive(Clone, Default)]
pub struct FakeExecutionGuard {
    state: Arc<Mutex<FakeExecutionGuardState>>,
    renew_every: Option<Duration>,
}

#[derive(Default)]
struct FakeExecutionGuardState {
    acquire_plan: VecDeque<AcquirePlan>,
    renew_plan: VecDeque<RenewPlan>,
    release_plan: VecDeque<ReleasePlan>,
    slots: Vec<ExecutionSlot>,
    acquire_count: usize,
}

impl FakeExecutionGuard {
    pub fn new(
        acquire_plan: impl IntoIterator<Item = AcquirePlan>,
        renew_plan: impl IntoIterator<Item = RenewPlan>,
        release_plan: impl IntoIterator<Item = ReleasePlan>,
        renew_every: Option<Duration>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeExecutionGuardState {
                acquire_plan: acquire_plan.into_iter().collect(),
                renew_plan: renew_plan.into_iter().collect(),
                release_plan: release_plan.into_iter().collect(),
                slots: Vec::new(),
                acquire_count: 0,
            })),
            renew_every,
        }
    }

    pub fn slots(&self) -> Vec<ExecutionSlot> {
        self.state.lock().unwrap().slots.clone()
    }

    pub fn acquire_count(&self) -> usize {
        self.state.lock().unwrap().acquire_count
    }
}

impl ExecutionGuard for FakeExecutionGuard {
    type Error = FakeGuardError;

    async fn acquire(&self, slot: ExecutionSlot) -> Result<ExecutionGuardAcquire, Self::Error> {
        let mut state = self.state.lock().unwrap();
        state.acquire_count += 1;
        state.slots.push(slot.clone());
        let plan = state
            .acquire_plan
            .pop_front()
            .unwrap_or(AcquirePlan::Acquired);

        match plan {
            AcquirePlan::Acquired => Ok(ExecutionGuardAcquire::Acquired(ExecutionLease::new(
                slot.job_id.clone(),
                slot.resource_id.clone(),
                slot.scope,
                slot.scheduled_at,
                format!("token-{}", state.acquire_count),
                match slot.scheduled_at {
                    Some(scheduled_at) => {
                        format!("lease:{}:{}", slot.resource_id, scheduled_at.to_rfc3339())
                    }
                    None => format!("lease:{}:resource", slot.resource_id),
                },
            ))),
            AcquirePlan::Contended => Ok(ExecutionGuardAcquire::Contended),
            AcquirePlan::Error(kind, message) => Err(FakeGuardError { kind, message }),
        }
    }

    async fn renew(&self, _lease: &ExecutionLease) -> Result<ExecutionGuardRenewal, Self::Error> {
        let plan = self
            .state
            .lock()
            .unwrap()
            .renew_plan
            .pop_front()
            .unwrap_or(RenewPlan::Renewed);

        match plan {
            RenewPlan::Renewed => Ok(ExecutionGuardRenewal::Renewed),
            RenewPlan::Lost => Ok(ExecutionGuardRenewal::Lost),
            RenewPlan::Error(kind, message) => Err(FakeGuardError { kind, message }),
        }
    }

    async fn release(&self, _lease: &ExecutionLease) -> Result<(), Self::Error> {
        let plan = self
            .state
            .lock()
            .unwrap()
            .release_plan
            .pop_front()
            .unwrap_or(ReleasePlan::Ok);

        match plan {
            ReleasePlan::Ok => Ok(()),
            ReleasePlan::Error(kind, message) => Err(FakeGuardError { kind, message }),
        }
    }

    fn classify_error(error: &Self::Error) -> ExecutionGuardErrorKind
    where
        Self: Sized,
    {
        error.kind()
    }

    fn renew_interval(&self, _lease: &ExecutionLease) -> Option<Duration> {
        self.renew_every
    }
}
