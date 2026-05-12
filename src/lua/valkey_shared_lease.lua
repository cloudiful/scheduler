local function cleanup_expired_occurrences(occurrence_index_key, now_millis)
    redis.call('ZREMRANGEBYSCORE', occurrence_index_key, '-inf', now_millis)
end

local function acquire_occurrence(resource_lock_key, lease_key, occurrence_index_key, now_millis, token, ttl_millis, expires_at_millis)
    cleanup_expired_occurrences(occurrence_index_key, now_millis)
    if redis.call('EXISTS', resource_lock_key) == 1 then
        return 0
    end
    local ok = redis.call('SET', lease_key, token, 'NX', 'PX', ttl_millis)
    if not ok then
        return 0
    end
    redis.call('ZADD', occurrence_index_key, expires_at_millis, lease_key)
    return 1
end

local function acquire_resource(resource_lock_key, occurrence_index_key, now_millis, token, ttl_millis)
    cleanup_expired_occurrences(occurrence_index_key, now_millis)
    if redis.call('EXISTS', resource_lock_key) == 1 then
        return 0
    end
    if redis.call('ZCARD', occurrence_index_key) > 0 then
        return 0
    end
    local ok = redis.call('SET', resource_lock_key, token, 'NX', 'PX', ttl_millis)
    if not ok then
        return 0
    end
    return 1
end

local function renew_occurrence(lease_key, occurrence_index_key, token, ttl_millis, expires_at_millis)
    if redis.call('GET', lease_key) == token then
        redis.call('PEXPIRE', lease_key, ttl_millis)
        redis.call('ZADD', occurrence_index_key, expires_at_millis, lease_key)
        return 1
    end
    redis.call('ZREM', occurrence_index_key, lease_key)
    return 0
end

local function renew_resource(resource_lock_key, token, ttl_millis)
    if redis.call('GET', resource_lock_key) == token then
        return redis.call('PEXPIRE', resource_lock_key, ttl_millis)
    end
    return 0
end

local function release_occurrence(lease_key, occurrence_index_key, token)
    if redis.call('GET', lease_key) == token then
        redis.call('DEL', lease_key)
        redis.call('ZREM', occurrence_index_key, lease_key)
        return 1
    end
    redis.call('ZREM', occurrence_index_key, lease_key)
    return 0
end

local function release_resource(resource_lock_key, token)
    if redis.call('GET', resource_lock_key) == token then
        return redis.call('DEL', resource_lock_key)
    end
    return 0
end
