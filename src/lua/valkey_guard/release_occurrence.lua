if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
    redis.call('ZREM', KEYS[2], KEYS[1])
    return 1
end
redis.call('ZREM', KEYS[2], KEYS[1])
return 0
