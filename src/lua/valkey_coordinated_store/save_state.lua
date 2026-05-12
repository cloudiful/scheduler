local version = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '-1')
local inflight = redis.call('HGET', KEYS[1], ARGV[3])
if inflight then
    return 0
end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[6])
if redis.call('ZCARD', KEYS[2]) > 0 then
    return 0
end
if version ~= tonumber(ARGV[2]) then
    return 0
end
redis.call('HSET', KEYS[1], ARGV[1], version + 1, ARGV[4], ARGV[5])
return 1
