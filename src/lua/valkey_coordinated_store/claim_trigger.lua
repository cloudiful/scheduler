local version = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '-1')
local inflight = redis.call('HGET', KEYS[1], ARGV[2])
local paused = redis.call('HGET', KEYS[1], ARGV[4])
if paused == '1' or paused == 'true' then
    return 0
end
if inflight then
    local inflight_expires_at = tonumber(redis.call('HGET', KEYS[1], ARGV[3]) or '0')
    if inflight_expires_at > tonumber(ARGV[5]) then
        return 0
    end
    return 0
end
if version ~= tonumber(ARGV[6]) then
    return 0
end
redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', ARGV[5])
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
local ok = redis.call('SET', KEYS[3], ARGV[7], 'NX', 'PX', ARGV[8])
if not ok then
    return 0
end
redis.call('ZADD', KEYS[4], ARGV[9], KEYS[3])
redis.call('HSET', KEYS[1],
    ARGV[1], version + 1,
    ARGV[10], ARGV[11],
    ARGV[12], ARGV[13],
    ARGV[14], ARGV[15],
    ARGV[16], ARGV[17],
    ARGV[18], ARGV[19],
    ARGV[20], ARGV[21],
    ARGV[22], ARGV[7],
    ARGV[23], KEYS[3],
    ARGV[3], ARGV[9]
)
return version + 1
