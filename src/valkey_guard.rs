use crate::error::{ExecutionGuardError, ExecutionGuardErrorKind};
use crate::valkey_execution_support::{
    lease_key, next_token, now_millis, occurrence_index_key, resource_lock_key,
};
use crate::{
    ExecutionGuard, ExecutionGuardAcquire, ExecutionGuardRenewal, ExecutionGuardScope,
    ExecutionLease, ExecutionSlot,
};
use redis::{Client, ErrorKind, Script, ServerErrorKind, aio::ConnectionManager};
use std::fmt::{self, Display, Formatter};
use std::num::TryFromIntError;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

const DEFAULT_KEY_PREFIX: &str = "scheduler:valkey:execution-lease:";
const ACQUIRE_OCCURRENCE_LUA: &str = include_str!("lua/valkey_guard/acquire_occurrence.lua");
const ACQUIRE_RESOURCE_LUA: &str = include_str!("lua/valkey_guard/acquire_resource.lua");
const RENEW_OCCURRENCE_LUA: &str = include_str!("lua/valkey_guard/renew_occurrence.lua");
const RENEW_RESOURCE_LUA: &str = include_str!("lua/valkey_guard/renew_resource.lua");
const RELEASE_OCCURRENCE_LUA: &str = include_str!("lua/valkey_guard/release_occurrence.lua");
const RELEASE_RESOURCE_LUA: &str = include_str!("lua/valkey_guard/release_resource.lua");

static TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValkeyLeaseConfig {
    pub ttl: Duration,
    pub renew_interval: Duration,
}

#[derive(Debug, Clone)]
pub struct ValkeyExecutionGuard {
    connection: ConnectionManager,
    key_prefix: String,
    lease_config: ValkeyLeaseConfig,
}

impl ValkeyExecutionGuard {
    pub async fn new(
        url: impl AsRef<str>,
        lease_config: ValkeyLeaseConfig,
    ) -> Result<Self, ExecutionGuardError> {
        Self::with_prefix(url, DEFAULT_KEY_PREFIX, lease_config).await
    }

    pub async fn with_prefix(
        url: impl AsRef<str>,
        key_prefix: impl Into<String>,
        lease_config: ValkeyLeaseConfig,
    ) -> Result<Self, ExecutionGuardError> {
        lease_config.validate()?;
        let client = Client::open(url.as_ref()).map_err(|error| {
            let kind = classify_redis_error(&error);
            ExecutionGuardError::new(error, kind)
        })?;
        let connection = client.get_connection_manager().await.map_err(|error| {
            let kind = classify_redis_error(&error);
            ExecutionGuardError::new(error, kind)
        })?;

        Ok(Self {
            connection,
            key_prefix: key_prefix.into(),
            lease_config,
        })
    }

    fn lease_key(&self, slot: &ExecutionSlot) -> String {
        lease_key(&self.key_prefix, slot)
    }

    fn resource_lock_key(&self, resource_id: &str) -> String {
        resource_lock_key(&self.key_prefix, resource_id)
    }

    fn occurrence_index_key(&self, resource_id: &str) -> String {
        occurrence_index_key(&self.key_prefix, resource_id)
    }

    fn ttl_millis(&self) -> Result<u64, ValkeyExecutionGuardError> {
        u64::try_from(self.lease_config.ttl.as_millis())
            .map_err(ValkeyExecutionGuardError::DurationOutOfRange)
    }
}

impl ExecutionGuard for ValkeyExecutionGuard {
    type Error = ValkeyExecutionGuardError;

    async fn acquire(&self, slot: ExecutionSlot) -> Result<ExecutionGuardAcquire, Self::Error> {
        let lease_key = self.lease_key(&slot);
        let token = next_token(&TOKEN_COUNTER, "lease");
        let ttl_millis = self.ttl_millis()?;
        let now_millis = now_millis();
        let expires_at_millis = now_millis.saturating_add(ttl_millis);
        let mut connection = self.connection.clone();
        let acquired = match slot.scope {
            ExecutionGuardScope::Occurrence => {
                let resource_lock_key = self.resource_lock_key(&slot.resource_id);
                let occurrence_index_key = self.occurrence_index_key(&slot.resource_id);
                let acquired: i32 = Script::new(ACQUIRE_OCCURRENCE_LUA)
                    .key(resource_lock_key)
                    .key(&lease_key)
                    .key(occurrence_index_key)
                    .arg(now_millis)
                    .arg(&token)
                    .arg(ttl_millis)
                    .arg(expires_at_millis)
                    .invoke_async(&mut connection)
                    .await
                    .map_err(ValkeyExecutionGuardError::Redis)?;
                acquired == 1
            }
            ExecutionGuardScope::Resource => {
                let resource_lock_key = self.resource_lock_key(&slot.resource_id);
                let occurrence_index_key = self.occurrence_index_key(&slot.resource_id);
                let acquired: i32 = Script::new(ACQUIRE_RESOURCE_LUA)
                    .key(&resource_lock_key)
                    .key(occurrence_index_key)
                    .arg(now_millis)
                    .arg(&token)
                    .arg(ttl_millis)
                    .invoke_async(&mut connection)
                    .await
                    .map_err(ValkeyExecutionGuardError::Redis)?;
                acquired == 1
            }
        };

        Ok(if acquired {
            ExecutionGuardAcquire::Acquired(ExecutionLease::new(
                slot.job_id,
                slot.resource_id,
                slot.scope,
                slot.scheduled_at,
                token,
                lease_key,
            ))
        } else {
            ExecutionGuardAcquire::Contended
        })
    }

    async fn renew(&self, lease: &ExecutionLease) -> Result<ExecutionGuardRenewal, Self::Error> {
        let ttl_millis = self.ttl_millis()?;
        let expires_at_millis = now_millis().saturating_add(ttl_millis);
        let mut connection = self.connection.clone();
        let renewed: i32 = match lease.scope {
            ExecutionGuardScope::Occurrence => Script::new(RENEW_OCCURRENCE_LUA)
                .key(&lease.lease_key)
                .key(self.occurrence_index_key(&lease.resource_id))
                .arg(&lease.token)
                .arg(ttl_millis)
                .arg(expires_at_millis)
                .invoke_async(&mut connection)
                .await
                .map_err(ValkeyExecutionGuardError::Redis)?,
            ExecutionGuardScope::Resource => Script::new(RENEW_RESOURCE_LUA)
                .key(&lease.lease_key)
                .arg(&lease.token)
                .arg(ttl_millis)
                .invoke_async(&mut connection)
                .await
                .map_err(ValkeyExecutionGuardError::Redis)?,
        };

        Ok(if renewed == 1 {
            ExecutionGuardRenewal::Renewed
        } else {
            ExecutionGuardRenewal::Lost
        })
    }

    async fn release(&self, lease: &ExecutionLease) -> Result<(), Self::Error> {
        let mut connection = self.connection.clone();
        let _: i32 = match lease.scope {
            ExecutionGuardScope::Occurrence => Script::new(RELEASE_OCCURRENCE_LUA)
                .key(&lease.lease_key)
                .key(self.occurrence_index_key(&lease.resource_id))
                .arg(&lease.token)
                .invoke_async(&mut connection)
                .await
                .map_err(ValkeyExecutionGuardError::Redis)?,
            ExecutionGuardScope::Resource => Script::new(RELEASE_RESOURCE_LUA)
                .key(&lease.lease_key)
                .arg(&lease.token)
                .invoke_async(&mut connection)
                .await
                .map_err(ValkeyExecutionGuardError::Redis)?,
        };

        Ok(())
    }

    fn classify_error(error: &Self::Error) -> ExecutionGuardErrorKind
    where
        Self: Sized,
    {
        match error {
            ValkeyExecutionGuardError::Config(_)
            | ValkeyExecutionGuardError::DurationOutOfRange(_) => ExecutionGuardErrorKind::Data,
            ValkeyExecutionGuardError::Redis(error) => classify_redis_error(error),
        }
    }

    fn renew_interval(&self, _lease: &ExecutionLease) -> Option<Duration> {
        Some(self.lease_config.renew_interval)
    }
}

#[derive(Debug)]
pub enum ValkeyExecutionGuardError {
    Redis(redis::RedisError),
    Config(LeaseConfigError),
    DurationOutOfRange(TryFromIntError),
}

impl Display for ValkeyExecutionGuardError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Redis(error) => write!(f, "{error}"),
            Self::Config(error) => write!(f, "{error}"),
            Self::DurationOutOfRange(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ValkeyExecutionGuardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Redis(error) => Some(error),
            Self::Config(error) => Some(error),
            Self::DurationOutOfRange(error) => Some(error),
        }
    }
}

#[derive(Debug)]
pub struct LeaseConfigError {
    message: &'static str,
}

impl Display for LeaseConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for LeaseConfigError {}

impl ValkeyLeaseConfig {
    fn validate(self) -> Result<Self, ExecutionGuardError> {
        if self.ttl.is_zero() {
            return Err(ExecutionGuardError::new(
                LeaseConfigError {
                    message: "execution guard ttl must be greater than zero",
                },
                ExecutionGuardErrorKind::Data,
            ));
        }

        if self.renew_interval.is_zero() {
            return Err(ExecutionGuardError::new(
                LeaseConfigError {
                    message: "execution guard renew_interval must be greater than zero",
                },
                ExecutionGuardErrorKind::Data,
            ));
        }

        if self.renew_interval >= self.ttl {
            return Err(ExecutionGuardError::new(
                LeaseConfigError {
                    message: "execution guard renew_interval must be less than ttl",
                },
                ExecutionGuardErrorKind::Data,
            ));
        }

        if u64::try_from(self.ttl.as_millis()).is_err() {
            return Err(ExecutionGuardError::new(
                LeaseConfigError {
                    message: "execution guard ttl is too large to encode in milliseconds",
                },
                ExecutionGuardErrorKind::Data,
            ));
        }

        Ok(self)
    }
}

fn classify_redis_error(error: &redis::RedisError) -> ExecutionGuardErrorKind {
    if error.is_connection_dropped()
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
    {
        ExecutionGuardErrorKind::Connection
    } else {
        ExecutionGuardErrorKind::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_KEY_PREFIX, lease_key, next_token};
    use crate::{ExecutionGuardScope, ExecutionSlot};
    use chrono::{TimeZone, Utc};

    #[test]
    fn generated_tokens_are_not_empty() {
        assert_ne!(next_token(&super::TOKEN_COUNTER, "lease"), "");
        assert_ne!(next_token(&super::TOKEN_COUNTER, "lease"), "");
    }

    #[test]
    fn lease_key_uses_default_prefix_and_occurrence_time() {
        let slot = ExecutionSlot::for_occurrence(
            "job-1",
            "resource-1",
            Utc.with_ymd_and_hms(2026, 4, 3, 1, 2, 3).unwrap(),
        );

        assert_eq!(
            lease_key(DEFAULT_KEY_PREFIX, &slot),
            "scheduler:valkey:execution-lease:resource-1:occurrence:2026-04-03T01:02:03.000000000Z"
        );
    }

    #[test]
    fn resource_scope_lease_key_uses_resource_lock() {
        let slot = ExecutionSlot::for_resource("job-1", "resource-1");

        assert_eq!(slot.scope, ExecutionGuardScope::Resource);
        assert_eq!(
            lease_key(DEFAULT_KEY_PREFIX, &slot),
            "scheduler:valkey:execution-lease:resource-1:resource"
        );
    }
}
