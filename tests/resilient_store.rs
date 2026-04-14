use chrono::{TimeDelta, Utc};
use scheduler::{
    Job, JobState, ResilientStateStore, ResilientStoreError, Schedule, Scheduler, SchedulerConfig,
    SchedulerEvent, SchedulerObserver, StateStore, StoreErrorKind, StoreOperation, Task,
};
use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestStoreError {
    Connection(&'static str),
    Data(&'static str),
}

impl Display for TestStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TestStoreError::Connection(message) | TestStoreError::Data(message) => message.fmt(f),
        }
    }
}

impl std::error::Error for TestStoreError {}

impl ResilientStoreError for TestStoreError {
    fn is_connection_issue(&self) -> bool {
        matches!(self, TestStoreError::Connection(_))
    }
}

#[derive(Debug, Default)]
struct TestStoreStats {
    load_calls: AtomicUsize,
    save_calls: AtomicUsize,
}

#[derive(Debug)]
struct ScriptedStore {
    loads: Mutex<VecDeque<Result<Option<JobState>, TestStoreError>>>,
    saves: Mutex<VecDeque<Result<(), TestStoreError>>>,
    stats: Arc<TestStoreStats>,
}

impl ScriptedStore {
    fn new(
        loads: impl Into<Vec<Result<Option<JobState>, TestStoreError>>>,
        saves: impl Into<Vec<Result<(), TestStoreError>>>,
    ) -> (Self, Arc<TestStoreStats>) {
        let stats = Arc::new(TestStoreStats::default());
        (
            Self {
                loads: Mutex::new(loads.into().into()),
                saves: Mutex::new(saves.into().into()),
                stats: stats.clone(),
            },
            stats,
        )
    }
}

impl StateStore for ScriptedStore {
    type Error = TestStoreError;

    async fn load(&self, _job_id: &str) -> Result<Option<JobState>, Self::Error> {
        self.stats.load_calls.fetch_add(1, Ordering::SeqCst);
        self.loads.lock().await.pop_front().unwrap_or(Ok(None))
    }

    async fn save(&self, _state: &JobState) -> Result<(), Self::Error> {
        self.stats.save_calls.fetch_add(1, Ordering::SeqCst);
        self.saves.lock().await.pop_front().unwrap_or(Ok(()))
    }

    fn classify_error(error: &Self::Error) -> StoreErrorKind
    where
        Self: Sized,
    {
        match error {
            TestStoreError::Connection(_) => StoreErrorKind::Connection,
            TestStoreError::Data(_) => StoreErrorKind::Data,
        }
    }
}

fn fixture_state(job_id: &str) -> JobState {
    JobState {
        job_id: job_id.to_string(),
        trigger_count: 7,
        last_run_at: Some(Utc::now()),
        last_success_at: Some(Utc::now() + TimeDelta::seconds(1)),
        next_run_at: Some(Utc::now() + TimeDelta::seconds(5)),
        last_error: Some("cached".to_string()),
    }
}

#[derive(Clone, Default)]
struct RecordingObserver {
    events: Arc<StdMutex<Vec<SchedulerEvent>>>,
}

impl RecordingObserver {
    fn snapshot(&self) -> Vec<SchedulerEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl SchedulerObserver for RecordingObserver {
    fn on_event(&self, event: &SchedulerEvent) {
        self.events.lock().unwrap().push(event.clone());
    }
}

#[tokio::test]
async fn scheduler_run_survives_store_connection_error() {
    let (remote, stats) = ScriptedStore::new(
        vec![Ok(None)],
        vec![Err(TestStoreError::Connection("connection reset"))],
    );
    let scheduler = Scheduler::new(SchedulerConfig::default(), ResilientStateStore::new(remote));
    let invocations = Arc::new(AtomicUsize::new(0));
    let seen = invocations.clone();

    let report = scheduler
        .run(
            Job::without_deps(
                "scheduler-store-fallback",
                Schedule::Interval(Duration::from_millis(1)),
                Task::from_async(move |_| {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }),
            )
            .with_max_runs(1),
        )
        .await
        .expect("scheduler should continue after store connection failure");

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert_eq!(report.history.len(), 1);
    assert_eq!(stats.load_calls.load(Ordering::SeqCst), 1);
    assert_eq!(stats.save_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn scheduler_run_still_fails_for_store_data_error() {
    let (remote, _stats) =
        ScriptedStore::new(vec![Err(TestStoreError::Data("invalid payload"))], vec![]);
    let scheduler = Scheduler::new(SchedulerConfig::default(), ResilientStateStore::new(remote));

    let error = scheduler
        .run(
            Job::without_deps(
                "scheduler-store-data-error",
                Schedule::Interval(Duration::from_millis(1)),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_max_runs(1),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        scheduler::SchedulerError::Store(ref store_error)
            if store_error.kind() == StoreErrorKind::Data
    ));
}

#[tokio::test]
async fn observer_receives_store_degraded_event() {
    let observer = RecordingObserver::default();
    let (remote, _stats) = ScriptedStore::new(
        vec![Ok(None)],
        vec![Err(TestStoreError::Connection("connection reset"))],
    );
    let scheduler = Scheduler::with_observer(
        SchedulerConfig::default(),
        ResilientStateStore::new(remote),
        observer.clone(),
    );

    scheduler
        .run(
            Job::without_deps(
                "scheduler-store-observer",
                Schedule::Interval(Duration::from_millis(1)),
                Task::from_async(|_| async { Ok(()) }),
            )
            .with_max_runs(1),
        )
        .await
        .expect("scheduler should continue after degrade");

    let events = observer.snapshot();
    assert!(events.iter().any(|event| {
        matches!(
            event,
            SchedulerEvent::StoreDegraded { job_id, operation, error }
                if job_id == "scheduler-store-observer"
                    && *operation == StoreOperation::Save
                    && error.contains("connection reset")
        )
    }));
}

#[tokio::test]
async fn resilient_load_returns_mirror_state_after_connection_error() {
    let state = fixture_state("load-mirror");
    let (remote, stats) = ScriptedStore::new(
        vec![
            Ok(Some(state.clone())),
            Err(TestStoreError::Connection("timeout")),
            Ok(Some(fixture_state("remote-should-not-run"))),
        ],
        vec![],
    );
    let store = ResilientStateStore::new(remote);

    assert_eq!(
        store.load(&state.job_id).await.unwrap(),
        Some(state.clone())
    );
    assert_eq!(
        store.load(&state.job_id).await.unwrap(),
        Some(state.clone())
    );
    assert_eq!(store.load(&state.job_id).await.unwrap(), Some(state));
    assert_eq!(stats.load_calls.load(Ordering::SeqCst), 2);
    assert!(store.is_degraded());
}
