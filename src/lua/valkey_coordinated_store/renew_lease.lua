local scope = ARGV[4]
local renewed = 0
if scope == 'resource' then
    renewed = renew_resource(KEYS[1], ARGV[1], ARGV[2])
else
    renewed = renew_occurrence(KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3])
end
if renewed == 1 then
    redis.call('ZADD', KEYS[4], ARGV[3], KEYS[1])
    redis.call('HSET', KEYS[3], 'inflight:' .. KEYS[1] .. ':expires_at', ARGV[3])
    return 1
end
redis.call('ZREM', KEYS[4], KEYS[1])
return 0
