local paused = redis.call('HGET', KEYS[1], ARGV[1])
if not paused then
    return 0
end
if paused == '0' or paused == 'false' then
    return 0
end
redis.call('HSET', KEYS[1], ARGV[1], '0')
return 1
