local version = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '-1')
local token = redis.call('HGET', KEYS[1], ARGV[2])
if version ~= tonumber(ARGV[3]) then
    return 0
end
if token ~= ARGV[4] then
    return 0
end
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], KEYS[2])
redis.call('HSET', KEYS[1], ARGV[1], version + 1, ARGV[5], ARGV[6])
redis.call('HDEL', KEYS[1], ARGV[2], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12])
return 1
