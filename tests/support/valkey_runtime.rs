use redis::Client;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn valkey_url() -> Option<String> {
    std::env::var("SCHEDULER_VALKEY_URL").ok()
}

pub fn unique_id(prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    format!("{prefix}-{}-{now}", std::process::id())
}

pub async fn connection(url: &str) -> redis::aio::MultiplexedConnection {
    Client::open(url)
        .expect("invalid valkey url")
        .get_multiplexed_async_connection()
        .await
        .expect("failed to get valkey connection")
}
