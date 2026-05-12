local field_version = ARGV[1]
local field_paused = ARGV[2]
local now_millis = tonumber(ARGV[3])
local expected_revision = tonumber(ARGV[4])
local token = ARGV[5]
local ttl_millis = ARGV[6]
local expires_at_millis = ARGV[7]
local field_state = ARGV[8]
local next_state_payload = ARGV[9]
local scheduled_at = ARGV[10]
local catch_up = ARGV[11]
local trigger_count = ARGV[12]
local resource_id = ARGV[13]
local scope = ARGV[14]

local version = tonumber(redis.call('HGET', KEYS[1], field_version) or '-1')
local paused = redis.call('HGET', KEYS[1], field_paused)
if paused == '1' or paused == 'true' then
    return 0
end
if version ~= expected_revision then
    return 0
end

local lease_key = KEYS[3]
local acquired = 0
if scope == 'resource' then
    lease_key = KEYS[2]
    acquired = acquire_resource(KEYS[2], KEYS[4], now_millis, token, ttl_millis)
else
    acquired = acquire_occurrence(KEYS[2], KEYS[3], KEYS[4], now_millis, token, ttl_millis, expires_at_millis)
end
if acquired ~= 1 then
    return 0
end

local field_prefix = 'inflight:' .. lease_key .. ':'
redis.call('ZADD', KEYS[5], expires_at_millis, lease_key)
redis.call('HSET', KEYS[1],
    field_version, version + 1,
    field_state, next_state_payload,
    field_prefix .. 'scheduled_at', scheduled_at,
    field_prefix .. 'catch_up', catch_up,
    field_prefix .. 'trigger_count', trigger_count,
    field_prefix .. 'resource_id', resource_id,
    field_prefix .. 'scope', scope,
    field_prefix .. 'token', token,
    field_prefix .. 'lease_key', lease_key,
    field_prefix .. 'expires_at', expires_at_millis
)
return version + 1
