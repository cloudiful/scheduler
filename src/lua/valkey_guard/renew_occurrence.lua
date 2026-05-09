if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    redis.call('ZADD', KEYS[2], ARGV[3], KEYS[1])
    return 1
end
redis.call('ZREM', KEYS[2], KEYS[1])
return 0
