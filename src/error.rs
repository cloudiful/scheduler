use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    InvalidJob(String),
    Store(String),
    TaskJoin(String),
}

impl Display for SchedulerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SchedulerError::InvalidJob(message) => write!(f, "invalid job: {message}"),
            SchedulerError::Store(message) => write!(f, "state store error: {message}"),
            SchedulerError::TaskJoin(message) => write!(f, "task join error: {message}"),
        }
    }
}

impl Error for SchedulerError {}
