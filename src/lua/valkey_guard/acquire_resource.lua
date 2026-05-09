redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
if redis.call('ZCARD', KEYS[2]) > 0 then
    return 0
end
local ok = redis.call('SET', KEYS[1], ARGV[2], 'NX', 'PX', ARGV[3])
if not ok then
    return 0
end
return 1
