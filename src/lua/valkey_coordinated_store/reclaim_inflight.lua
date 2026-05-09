local field_scheduled_at = ARGV[1]
local field_catch_up = ARGV[2]
local field_trigger_count = ARGV[3]
local field_resource_id = ARGV[4]
local field_scope = ARGV[5]
local field_lease_expires_at = ARGV[6]
local field_state = ARGV[7]
local field_version = ARGV[8]
local field_paused = ARGV[9]
local now_millis = tonumber(ARGV[10])
local occurrence_prefix = ARGV[11]
local token = ARGV[12]
local ttl_millis = ARGV[13]
local expires_at_millis = ARGV[14]
local field_token = ARGV[15]
local field_lease_key = ARGV[16]

local scheduled_at = redis.call('HGET', KEYS[1], field_scheduled_at)
local catch_up = redis.call('HGET', KEYS[1], field_catch_up)
local trigger_count = redis.call('HGET', KEYS[1], field_trigger_count)
local inflight_resource_id = redis.call('HGET', KEYS[1], field_resource_id)
local inflight_scope = redis.call('HGET', KEYS[1], field_scope)
local inflight_expires_at = tonumber(redis.call('HGET', KEYS[1], field_lease_expires_at) or '0')
local state_payload = redis.call('HGET', KEYS[1], field_state)
local version = tonumber(redis.call('HGET', KEYS[1], field_version) or '0')
local paused = redis.call('HGET', KEYS[1], field_paused)

if not scheduled_at or not inflight_resource_id or not inflight_scope then
    return nil
end
if paused == '1' or paused == 'true' then
    return nil
end
if inflight_expires_at > now_millis then
    return nil
end
redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', now_millis)
if redis.call('EXISTS', KEYS[2]) == 1 then
    return nil
end
local new_lease_key = occurrence_prefix .. scheduled_at
local ok = redis.call('SET', new_lease_key, token, 'NX', 'PX', ttl_millis)
if not ok then
    return nil
end
redis.call('ZADD', KEYS[4], expires_at_millis, new_lease_key)
redis.call('HSET', KEYS[1],
    field_lease_expires_at, expires_at_millis,
    field_token, token,
    field_lease_key, new_lease_key,
    field_version, version + 1
)
return { tostring(version + 1), state_payload, scheduled_at, catch_up, trigger_count, inflight_scope, new_lease_key, token }
