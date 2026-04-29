use std::error::Error;
use std::fmt::{self, Display, Formatter};
use tokio::task::JoinError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidJobKind {
    ZeroInterval,
    IntervalOutOfRange,
    CronExpression,
    Other,
}

#[derive(Debug)]
pub struct InvalidJobError {
    kind: InvalidJobKind,
    message: String,
}

impl InvalidJobError {
    pub fn kind(&self) -> InvalidJobKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for InvalidJobError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for InvalidJobError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreErrorKind {
    Connection,
    Data,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionGuardErrorKind {
    Connection,
    Data,
    Unknown,
}

#[derive(Debug)]
pub struct StoreError {
    kind: StoreErrorKind,
    source: Box<dyn Error + Send + Sync>,
}

impl StoreError {
    pub fn new<E>(source: E, kind: StoreErrorKind) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind,
            source: Box::new(source),
        }
    }

    pub fn kind(&self) -> StoreErrorKind {
        self.kind
    }
}

impl Display for StoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug)]
pub struct ExecutionGuardError {
    kind: ExecutionGuardErrorKind,
    source: Box<dyn Error + Send + Sync>,
}

impl ExecutionGuardError {
    pub fn new<E>(source: E, kind: ExecutionGuardErrorKind) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind,
            source: Box::new(source),
        }
    }

    pub fn kind(&self) -> ExecutionGuardErrorKind {
        self.kind
    }
}

impl Display for ExecutionGuardError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl Error for ExecutionGuardError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskJoinErrorKind {
    Cancelled,
    Panic,
    Unknown,
}

#[derive(Debug)]
pub struct TaskJoinError {
    kind: TaskJoinErrorKind,
    message: String,
}

impl TaskJoinError {
    pub fn kind(&self) -> TaskJoinErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for TaskJoinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for TaskJoinError {}

#[derive(Debug)]
pub enum SchedulerError {
    InvalidJob(InvalidJobError),
    Store(StoreError),
    ExecutionGuard(ExecutionGuardError),
    TaskJoin(TaskJoinError),
}

impl Display for SchedulerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SchedulerError::InvalidJob(message) => write!(f, "invalid job: {message}"),
            SchedulerError::Store(error) => write!(f, "state store error: {error}"),
            SchedulerError::ExecutionGuard(error) => {
                write!(f, "execution guard error: {error}")
            }
            SchedulerError::TaskJoin(message) => write!(f, "task join error: {message}"),
        }
    }
}

impl Error for SchedulerError {}

impl SchedulerError {
    pub(crate) fn invalid_zero_interval() -> Self {
        Self::invalid_job_with_kind(
            InvalidJobKind::ZeroInterval,
            "interval schedule must be greater than zero",
        )
    }

    pub(crate) fn invalid_interval_out_of_range() -> Self {
        Self::invalid_job_with_kind(
            InvalidJobKind::IntervalOutOfRange,
            "interval schedule is too large to represent",
        )
    }

    pub(crate) fn invalid_cron(message: impl Into<String>) -> Self {
        Self::invalid_job_with_kind(InvalidJobKind::CronExpression, message)
    }

    pub(crate) fn invalid_job_with_kind(kind: InvalidJobKind, message: impl Into<String>) -> Self {
        Self::InvalidJob(InvalidJobError {
            kind,
            message: message.into(),
        })
    }

    pub(crate) fn store<E>(error: E, kind: StoreErrorKind) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Store(StoreError::new(error, kind))
    }

    pub(crate) fn task_join(error: JoinError) -> Self {
        let kind = if error.is_cancelled() {
            TaskJoinErrorKind::Cancelled
        } else if error.is_panic() {
            TaskJoinErrorKind::Panic
        } else {
            TaskJoinErrorKind::Unknown
        };

        Self::TaskJoin(TaskJoinError {
            kind,
            message: error.to_string(),
        })
    }

    pub(crate) fn execution_guard<E>(error: E, kind: ExecutionGuardErrorKind) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::ExecutionGuard(ExecutionGuardError::new(error, kind))
    }
}
