local field_state = ARGV[1]
local field_version = ARGV[2]
local field_paused = ARGV[3]
local now_millis = tonumber(ARGV[4])
local token = ARGV[5]
local ttl_millis = ARGV[6]
local expires_at_millis = ARGV[7]

local paused = redis.call('HGET', KEYS[1], field_paused)
if paused == '1' or paused == 'true' then
    return nil
end

local expired = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now_millis, 'LIMIT', 0, 1)
local lease_key = expired[1]
local scheduled_at = nil
local catch_up = nil
local trigger_count = nil
local inflight_resource_id = nil
local inflight_scope = nil

if lease_key then
    local field_prefix = 'inflight:' .. lease_key .. ':'
    scheduled_at = redis.call('HGET', KEYS[1], field_prefix .. 'scheduled_at')
    catch_up = redis.call('HGET', KEYS[1], field_prefix .. 'catch_up')
    trigger_count = redis.call('HGET', KEYS[1], field_prefix .. 'trigger_count')
    inflight_resource_id = redis.call('HGET', KEYS[1], field_prefix .. 'resource_id')
    inflight_scope = redis.call('HGET', KEYS[1], field_prefix .. 'scope')
end

if not lease_key then
    scheduled_at = redis.call('HGET', KEYS[1], ARGV[8])
    catch_up = redis.call('HGET', KEYS[1], ARGV[9])
    trigger_count = redis.call('HGET', KEYS[1], ARGV[10])
    inflight_resource_id = redis.call('HGET', KEYS[1], ARGV[11])
    inflight_scope = redis.call('HGET', KEYS[1], ARGV[12])
    lease_key = redis.call('HGET', KEYS[1], ARGV[13])
    local legacy_expires_at = tonumber(redis.call('HGET', KEYS[1], ARGV[14]) or '0')
    if not lease_key or legacy_expires_at > now_millis then
        return nil
    end
end

if not scheduled_at or not inflight_resource_id or not inflight_scope then
    if lease_key then
        redis.call('ZREM', KEYS[4], lease_key)
    end
    return nil
end

local acquired = 0
if inflight_scope == 'resource' then
    acquired = acquire_resource(KEYS[2], KEYS[3], now_millis, token, ttl_millis)
    lease_key = KEYS[2]
else
    acquired = acquire_occurrence(KEYS[2], lease_key, KEYS[3], now_millis, token, ttl_millis, expires_at_millis)
end
if acquired ~= 1 then
    return nil
end

local field_prefix = 'inflight:' .. lease_key .. ':'
redis.call('ZADD', KEYS[4], expires_at_millis, lease_key)
redis.call('HSET', KEYS[1],
    field_prefix .. 'scheduled_at', scheduled_at,
    field_prefix .. 'catch_up', catch_up,
    field_prefix .. 'trigger_count', trigger_count,
    field_prefix .. 'resource_id', inflight_resource_id,
    field_prefix .. 'scope', inflight_scope,
    field_prefix .. 'token', token,
    field_prefix .. 'lease_key', lease_key,
    field_prefix .. 'expires_at', expires_at_millis
)

local version = tonumber(redis.call('HGET', KEYS[1], field_version) or '0')
redis.call('HSET', KEYS[1], field_version, version + 1)
redis.call('HDEL', KEYS[1], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13], ARGV[14], ARGV[15])
return { tostring(version + 1), redis.call('HGET', KEYS[1], field_state), scheduled_at, catch_up, trigger_count, inflight_scope, lease_key, token }
