use redis::AsyncCommands;

pub async fn delete_matching_prefix(
    connection: &mut redis::aio::MultiplexedConnection,
    prefix: &str,
) {
    let pattern = format!("{prefix}*");
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(pattern)
        .query_async(connection)
        .await
        .expect("failed to list prefixed keys");
    if !keys.is_empty() {
        let _: usize = connection.del(keys).await.expect("failed to cleanup keys");
    }
}
