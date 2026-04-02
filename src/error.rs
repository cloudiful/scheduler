use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Debug)]
pub struct StoreError {
    source: Box<dyn Error + Send + Sync>,
}

impl StoreError {
    pub fn new<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            source: Box::new(source),
        }
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
pub enum SchedulerError {
    InvalidJob(String),
    Store(StoreError),
    TaskJoin(String),
}

impl Display for SchedulerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SchedulerError::InvalidJob(message) => write!(f, "invalid job: {message}"),
            SchedulerError::Store(error) => write!(f, "state store error: {error}"),
            SchedulerError::TaskJoin(message) => write!(f, "task join error: {message}"),
        }
    }
}

impl Error for SchedulerError {}

impl SchedulerError {
    pub(crate) fn invalid_job(message: impl Into<String>) -> Self {
        Self::InvalidJob(message.into())
    }

    pub(crate) fn store<E>(error: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Store(StoreError::new(error))
    }

    pub(crate) fn task_join(message: impl Into<String>) -> Self {
        Self::TaskJoin(message.into())
    }
}
