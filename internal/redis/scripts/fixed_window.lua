-- KEYS[1]: The rate limit key (e.g. rl:user:123)
-- ARGV[1]: The limit (e.g. 100)
-- ARGV[2]: The window duration in seconds (e.g. 60)
-- ARGV[3]: The request cost (e.g. 1)

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

-- Get the current count
local current = redis.call("GET", key)
if current == false then
    current = 0
else
    current = tonumber(current)
end

-- Check if adding cost exceeds limit
if current + cost > limit then
    -- Rejected
    local ttl = redis.call("PTTL", key)
    if ttl == -1 then
        ttl = window * 1000
    elseif ttl == -2 then
        ttl = 0
    end
    return {0, limit - current, ttl}
end

-- Allowed: Increment the key
local new_count = redis.call("INCRBY", key, cost)

-- Set expiration if it's the first request in the window
if new_count == cost then
    redis.call("EXPIRE", key, window)
end

local ttl = redis.call("PTTL", key)
return {1, limit - new_count, ttl}
