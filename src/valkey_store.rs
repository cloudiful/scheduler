use crate::model::JobState;
use crate::store::StateStore;
use redis::{AsyncCommands, Client, aio::ConnectionManager};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

const DEFAULT_KEY_PREFIX: &str = "scheduler:valkey:job-state:";
const LEGACY_DEFAULT_KEY_PREFIX: &str = "scheduler:job-state:";

#[derive(Debug, Clone)]
pub struct ValkeyStateStore {
    connection: ConnectionManager,
    key_prefix: String,
}

impl ValkeyStateStore {
    pub async fn new(url: impl AsRef<str>) -> Result<Self, redis::RedisError> {
        Self::with_prefix(url, DEFAULT_KEY_PREFIX).await
    }

    pub async fn with_prefix(
        url: impl AsRef<str>,
        key_prefix: impl Into<String>,
    ) -> Result<Self, redis::RedisError> {
        let client = Client::open(url.as_ref())?;
        Self::from_client(client, key_prefix).await
    }

    pub async fn from_client(
        client: Client,
        key_prefix: impl Into<String>,
    ) -> Result<Self, redis::RedisError> {
        let connection = client.get_connection_manager().await?;
        Ok(Self {
            connection,
            key_prefix: key_prefix.into(),
        })
    }

    fn state_key(&self, job_id: &str) -> String {
        state_key(&self.key_prefix, job_id)
    }

    fn legacy_state_key(&self, job_id: &str) -> Option<String> {
        if self.key_prefix == DEFAULT_KEY_PREFIX {
            Some(state_key(LEGACY_DEFAULT_KEY_PREFIX, job_id))
        } else {
            None
        }
    }
}

fn state_key(prefix: &str, job_id: &str) -> String {
    format!("{prefix}{job_id}")
}

#[derive(Debug)]
pub enum ValkeyStoreError {
    Redis(redis::RedisError),
    Codec(serde_json::Error),
}

impl Display for ValkeyStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ValkeyStoreError::Redis(error) => write!(f, "{error}"),
            ValkeyStoreError::Codec(error) => write!(f, "{error}"),
        }
    }
}

impl Error for ValkeyStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ValkeyStoreError::Redis(error) => Some(error),
            ValkeyStoreError::Codec(error) => Some(error),
        }
    }
}

impl From<redis::RedisError> for ValkeyStoreError {
    fn from(error: redis::RedisError) -> Self {
        Self::Redis(error)
    }
}

impl From<serde_json::Error> for ValkeyStoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Codec(error)
    }
}

impl StateStore for ValkeyStateStore {
    type Error = ValkeyStoreError;

    async fn load(&self, job_id: &str) -> Result<Option<JobState>, Self::Error> {
        let mut connection = self.connection.clone();
        let payload: Option<String> = connection
            .get(self.state_key(job_id))
            .await
            .map_err(ValkeyStoreError::from)?;

        let payload = match payload {
            Some(payload) => Some(payload),
            None => {
                if let Some(legacy_key) = self.legacy_state_key(job_id) {
                    connection
                        .get(legacy_key)
                        .await
                        .map_err(ValkeyStoreError::from)?
                } else {
                    None
                }
            }
        };

        payload
            .map(|value| serde_json::from_str(&value).map_err(ValkeyStoreError::from))
            .transpose()
    }

    async fn save(&self, state: &JobState) -> Result<(), Self::Error> {
        let mut connection = self.connection.clone();
        let payload = serde_json::to_string(state).map_err(ValkeyStoreError::from)?;
        connection
            .set(self.state_key(&state.job_id), payload)
            .await
            .map_err(ValkeyStoreError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_KEY_PREFIX, LEGACY_DEFAULT_KEY_PREFIX, state_key};
    use crate::model::JobState;
    use chrono::{TimeDelta, Utc};

    #[test]
    fn state_key_uses_custom_prefix() {
        assert_eq!(state_key("custom:", "job-1"), "custom:job-1");
        assert_eq!(
            state_key(DEFAULT_KEY_PREFIX, "job-2"),
            "scheduler:valkey:job-state:job-2"
        );
    }

    #[test]
    fn legacy_default_prefix_is_stable() {
        assert_eq!(
            state_key(LEGACY_DEFAULT_KEY_PREFIX, "job-3"),
            "scheduler:job-state:job-3"
        );
    }

    #[test]
    fn job_state_json_round_trip() {
        let state = JobState {
            job_id: "job-1".to_string(),
            trigger_count: 2,
            last_run_at: Some(Utc::now()),
            last_success_at: Some(Utc::now() + TimeDelta::seconds(1)),
            next_run_at: Some(Utc::now() + TimeDelta::seconds(5)),
            last_error: Some("boom".to_string()),
        };

        let encoded = serde_json::to_string(&state).unwrap();
        let decoded: JobState = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, state);
    }
}
