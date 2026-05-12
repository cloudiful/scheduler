use chrono::{DateTime, Utc};
use scheduler::{
    CoordinatedClaim, CoordinatedCompletion, CoordinatedLeaseConfig, CoordinatedPendingTrigger,
    CoordinatedRuntimeState, CoordinatedStateStore, ExecutionGuard, ExecutionGuardAcquire,
    ExecutionGuardRenewal, ExecutionGuardScope, ExecutionLease, ExecutionSlot, JobState,
    SchedulerEvent, SchedulerObserver,
};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Default)]
pub struct InMemoryScopeGuard {
    state: Arc<Mutex<GuardState>>,
}

#[derive(Default)]
struct GuardState {
    resource_locks: HashSet<String>,
    occurrence_locks: HashMap<String, HashSet<String>>,
}

impl ExecutionGuard for InMemoryScopeGuard {
    type Error = Infallible;

    async fn acquire(&self, slot: ExecutionSlot) -> Result<ExecutionGuardAcquire, Self::Error> {
        let mut state = self.state.lock().unwrap();
        let resource_locked = state.resource_locks.contains(&slot.resource_id);

        let acquired = match slot.scope {
            ExecutionGuardScope::Occurrence => {
                if resource_locked {
                    false
                } else {
                    state
                        .occurrence_locks
                        .entry(slot.resource_id.clone())
                        .or_default()
                        .insert(slot.scheduled_at.expect("occurrence slot").to_rfc3339())
                }
            }
            ExecutionGuardScope::Resource => {
                let has_occurrences = state
                    .occurrence_locks
                    .get(&slot.resource_id)
                    .map(|entries| !entries.is_empty())
                    .unwrap_or(false);
                if resource_locked || has_occurrences {
                    false
                } else {
                    state.resource_locks.insert(slot.resource_id.clone())
                }
            }
        };

        Ok(if acquired {
            ExecutionGuardAcquire::Acquired(ExecutionLease::new(
                slot.job_id,
                slot.resource_id,
                slot.scope,
                slot.scheduled_at,
                "token",
                "lease",
            ))
        } else {
            ExecutionGuardAcquire::Contended
        })
    }

    async fn renew(&self, _lease: &ExecutionLease) -> Result<ExecutionGuardRenewal, Self::Error> {
        Ok(ExecutionGuardRenewal::Renewed)
    }

    async fn release(&self, lease: &ExecutionLease) -> Result<(), Self::Error> {
        let mut state = self.state.lock().unwrap();
        match lease.scope {
            ExecutionGuardScope::Occurrence => {
                if let Some(entries) = state.occurrence_locks.get_mut(&lease.resource_id) {
                    if let Some(scheduled_at) = lease.scheduled_at {
                        entries.remove(&scheduled_at.to_rfc3339());
                    }
                }
            }
            ExecutionGuardScope::Resource => {
                state.resource_locks.remove(&lease.resource_id);
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct FakeCoordinatedStore {
    inner: Arc<Mutex<FakeCoordinatedStoreState>>,
}

#[derive(Clone)]
struct FakeCoordinatedStoreState {
    runtime: CoordinatedRuntimeState,
    inflight: Vec<FakeInflight>,
}

#[derive(Clone)]
struct FakeInflight {
    trigger: CoordinatedPendingTrigger,
    resource_id: String,
    lease: ExecutionLease,
    expires_at: Instant,
}

impl FakeCoordinatedStore {
    pub fn new(state: JobState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeCoordinatedStoreState {
                runtime: CoordinatedRuntimeState {
                    state,
                    revision: 0,
                    paused: false,
                },
                inflight: Vec::new(),
            })),
        }
    }

    pub fn is_paused(&self) -> bool {
        self.inner.lock().unwrap().runtime.paused
    }

    pub fn reset_runtime_for_test(
        &self,
        next_run_at: Option<DateTime<Utc>>,
        trigger_count: u32,
        paused: bool,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.runtime.state.next_run_at = next_run_at;
        inner.runtime.state.trigger_count = trigger_count;
        inner.runtime.paused = paused;
    }
}

impl CoordinatedStateStore for FakeCoordinatedStore {
    type Error = Infallible;

    async fn load_or_initialize(
        &self,
        _job_id: &str,
        _initial_state: JobState,
    ) -> Result<CoordinatedRuntimeState, Self::Error> {
        Ok(self.inner.lock().unwrap().runtime.clone())
    }

    async fn save_state(
        &self,
        _job_id: &str,
        revision: u64,
        state: &JobState,
    ) -> Result<bool, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        if inner.runtime.revision != revision || !inner.inflight.is_empty() {
            return Ok(false);
        }
        inner.runtime.revision += 1;
        inner.runtime.state = state.clone();
        Ok(true)
    }

    async fn reclaim_inflight(
        &self,
        job_id: &str,
        resource_id: &str,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        if inner.runtime.paused {
            return Ok(None);
        }
        let Some(position) = inner
            .inflight
            .iter()
            .position(|inflight| inflight.expires_at <= Instant::now())
        else {
            return Ok(None);
        };
        let inflight = inner.inflight[position].clone();

        inner.runtime.revision += 1;
        let lease = ExecutionLease::new(
            job_id.to_string(),
            resource_id.to_string(),
            inflight.lease.scope,
            inflight.lease.scheduled_at,
            "reclaimed-token",
            "reclaimed-lease",
        );
        inner.inflight[position] = FakeInflight {
            trigger: inflight.trigger.clone(),
            resource_id: inflight.resource_id,
            expires_at: Instant::now() + lease_config.ttl,
            lease: lease.clone(),
        };

        Ok(Some(CoordinatedClaim {
            state: inner.runtime.clone(),
            trigger: inflight.trigger,
            lease,
            replayed: true,
        }))
    }

    async fn claim_trigger(
        &self,
        job_id: &str,
        resource_id: &str,
        revision: u64,
        trigger: CoordinatedPendingTrigger,
        next_state: &JobState,
        lease_config: CoordinatedLeaseConfig,
        scope: ExecutionGuardScope,
    ) -> Result<Option<CoordinatedClaim>, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        if inner.runtime.paused || inner.runtime.revision != revision {
            return Ok(None);
        }
        let resource_busy = inner
            .inflight
            .iter()
            .any(|inflight| inflight.resource_id == resource_id);
        let same_occurrence_busy = inner.inflight.iter().any(|inflight| {
            inflight.resource_id == resource_id
                && inflight.lease.scope == ExecutionGuardScope::Occurrence
                && inflight.trigger.scheduled_at == trigger.scheduled_at
        });
        if matches!(scope, ExecutionGuardScope::Resource) && resource_busy {
            return Ok(None);
        }
        if matches!(scope, ExecutionGuardScope::Occurrence)
            && (same_occurrence_busy
                || inner.inflight.iter().any(|inflight| {
                    inflight.resource_id == resource_id
                        && inflight.lease.scope == ExecutionGuardScope::Resource
                }))
        {
            return Ok(None);
        }

        inner.runtime.revision += 1;
        inner.runtime.state = next_state.clone();
        let token = format!("claim-token-{}", inner.runtime.revision);
        let lease = ExecutionLease::new(
            job_id.to_string(),
            resource_id.to_string(),
            scope,
            (scope == ExecutionGuardScope::Occurrence).then_some(trigger.scheduled_at),
            token.clone(),
            format!("claim-lease-{token}"),
        );
        inner.inflight.push(FakeInflight {
            trigger: trigger.clone(),
            resource_id: resource_id.to_string(),
            expires_at: Instant::now() + lease_config.ttl,
            lease: lease.clone(),
        });

        Ok(Some(CoordinatedClaim {
            state: inner.runtime.clone(),
            trigger,
            lease,
            replayed: false,
        }))
    }

    async fn pause(&self, _job_id: &str) -> Result<bool, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        let changed = !inner.runtime.paused;
        inner.runtime.paused = true;
        Ok(changed)
    }

    async fn resume(&self, _job_id: &str) -> Result<bool, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        let changed = inner.runtime.paused;
        inner.runtime.paused = false;
        Ok(changed)
    }

    async fn renew(
        &self,
        lease: &ExecutionLease,
        lease_config: CoordinatedLeaseConfig,
    ) -> Result<ExecutionGuardRenewal, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        let Some(inflight) = inner
            .inflight
            .iter_mut()
            .find(|inflight| inflight.lease.token == lease.token)
        else {
            return Ok(ExecutionGuardRenewal::Lost);
        };
        inflight.expires_at = Instant::now() + lease_config.ttl;
        Ok(ExecutionGuardRenewal::Renewed)
    }

    async fn complete(
        &self,
        _job_id: &str,
        lease: &ExecutionLease,
        completion: CoordinatedCompletion,
    ) -> Result<bool, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        let Some(position) = inner
            .inflight
            .iter()
            .position(|value| value.lease.token == lease.token)
        else {
            return Ok(false);
        };
        inner.runtime.revision += 1;
        inner.runtime.state.last_run_at = Some(completion.last_run_at);
        inner.runtime.state.last_success_at = completion.last_success_at;
        inner.runtime.state.last_error = completion.last_error;
        inner.inflight.remove(position);
        Ok(true)
    }

    async fn delete(&self, _job_id: &str) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.runtime.state.next_run_at = None;
        inner.inflight.clear();
        Ok(())
    }
}

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
