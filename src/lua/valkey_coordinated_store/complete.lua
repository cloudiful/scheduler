local lease_key = KEYS[2]
local field_prefix = 'inflight:' .. lease_key .. ':'
local token = redis.call('HGET', KEYS[1], field_prefix .. 'token')
local scope = redis.call('HGET', KEYS[1], field_prefix .. 'scope')
if not token then
    token = redis.call('HGET', KEYS[1], ARGV[7])
    scope = redis.call('HGET', KEYS[1], ARGV[8]) or 'occurrence'
end
if token ~= ARGV[1] then
    return 0
end
if scope == 'resource' then
    release_resource(lease_key, ARGV[1])
else
    release_occurrence(lease_key, KEYS[3], ARGV[1])
end
redis.call('ZREM', KEYS[4], lease_key)
local version = tonumber(redis.call('HGET', KEYS[1], ARGV[2]) or '0')
local state_payload = redis.call('HGET', KEYS[1], ARGV[3])
if state_payload then
    local state = cjson.decode(state_payload)
    state['last_run_at'] = ARGV[4]
    if ARGV[5] == '' then
        state['last_success_at'] = cjson.null
    else
        state['last_success_at'] = ARGV[5]
    end
    if ARGV[6] == '' then
        state['last_error'] = cjson.null
    else
        state['last_error'] = ARGV[6]
    end
    redis.call('HSET', KEYS[1], ARGV[2], version + 1, ARGV[3], cjson.encode(state))
else
    redis.call('HSET', KEYS[1], ARGV[2], version + 1)
end
redis.call('HDEL', KEYS[1],
    field_prefix .. 'scheduled_at',
    field_prefix .. 'catch_up',
    field_prefix .. 'trigger_count',
    field_prefix .. 'resource_id',
    field_prefix .. 'scope',
    field_prefix .. 'token',
    field_prefix .. 'lease_key',
    field_prefix .. 'expires_at',
    ARGV[7], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[8], ARGV[13]
)
return 1
