local paused = redis.call('HGET', KEYS[1], ARGV[1])
if not paused then
    return 0
end
if paused == '1' or paused == 'true' then
    return 0
end
redis.call('HSET', KEYS[1], ARGV[1], '1')
return 1
