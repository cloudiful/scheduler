redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', ARGV[1])
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
local ok = redis.call('SET', KEYS[2], ARGV[2], 'NX', 'PX', ARGV[3])
if not ok then
    return 0
end
redis.call('ZADD', KEYS[3], ARGV[4], KEYS[2])
return 1
