use redis::{Client, ErrorKind, ServerErrorKind, aio::ConnectionManager};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValkeyRecoveryConfig {
    pub retry_attempts: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub probe_interval: Duration,
}

impl Default for ValkeyRecoveryConfig {
    fn default() -> Self {
        Self {
            retry_attempts: 3,
            initial_backoff: Duration::from_millis(25),
            max_backoff: Duration::from_millis(250),
            probe_interval: Duration::from_millis(250),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ValkeyRuntimeEvent {
    Degraded { error: String },
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ValkeyCommandOutcome<T> {
    Available(T),
    Degraded,
}

#[derive(Debug, Clone)]
pub(crate) struct ValkeyRuntime {
    connection: ConnectionManager,
    config: ValkeyRecoveryConfig,
    degraded: Arc<std::sync::atomic::AtomicBool>,
    events: Arc<Mutex<VecDeque<ValkeyRuntimeEvent>>>,
}

impl ValkeyRuntime {
    pub(crate) async fn from_client(
        client: Client,
        config: ValkeyRecoveryConfig,
    ) -> Result<Self, redis::RedisError> {
        let connection = client.get_connection_manager().await?;
        Ok(Self {
            connection,
            config,
            degraded: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            events: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    pub(crate) async fn execute<T, F, Fut>(
        &self,
        mut operation: F,
    ) -> Result<ValkeyCommandOutcome<T>, redis::RedisError>
    where
        F: FnMut(ConnectionManager) -> Fut,
        Fut: Future<Output = Result<T, redis::RedisError>>,
    {
        let attempts = self.config.retry_attempts.max(1);
        let mut backoff = self.config.initial_backoff;
        let mut last_connection_error = None;

        for attempt in 0..attempts {
            let result = operation(self.connection.clone()).await;
            match result {
                Ok(value) => {
                    if self
                        .degraded
                        .swap(false, std::sync::atomic::Ordering::SeqCst)
                    {
                        self.events
                            .lock()
                            .await
                            .push_back(ValkeyRuntimeEvent::Recovered);
                    }
                    return Ok(ValkeyCommandOutcome::Available(value));
                }
                Err(error) if is_connection_issue(&error) => {
                    last_connection_error = Some(error);
                    if attempt + 1 < attempts {
                        tokio::time::sleep(backoff).await;
                        backoff = std::cmp::min(backoff.saturating_mul(2), self.config.max_backoff);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        if let Some(error) = last_connection_error {
            if !self
                .degraded
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                self.events
                    .lock()
                    .await
                    .push_back(ValkeyRuntimeEvent::Degraded {
                        error: error.to_string(),
                    });
            }
            Ok(ValkeyCommandOutcome::Degraded)
        } else {
            Ok(ValkeyCommandOutcome::Degraded)
        }
    }

    pub(crate) async fn drain_events(&self) -> Vec<ValkeyRuntimeEvent> {
        let mut events = self.events.lock().await;
        events.drain(..).collect()
    }
}

pub(crate) fn is_connection_issue(error: &redis::RedisError) -> bool {
    error.is_connection_dropped()
        || error.is_connection_refusal()
        || error.is_timeout()
        || matches!(
            error.kind(),
            ErrorKind::Io
                | ErrorKind::ClusterConnectionNotFound
                | ErrorKind::Server(ServerErrorKind::BusyLoading)
                | ErrorKind::Server(ServerErrorKind::ClusterDown)
                | ErrorKind::Server(ServerErrorKind::MasterDown)
                | ErrorKind::Server(ServerErrorKind::TryAgain)
        )
}
