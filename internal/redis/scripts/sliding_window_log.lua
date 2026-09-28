-- KEYS[1]: The rate limit key
-- ARGV[1]: limit
-- ARGV[2]: window in milliseconds
-- ARGV[3]: request cost
-- ARGV[4]: unique request ID base

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local base_id = ARGV[4]

local time_arr = redis.call("TIME")
local now_ms = tonumber(time_arr[1]) * 1000 + math.floor(tonumber(time_arr[2]) / 1000)

local window_start = now_ms - window_ms

-- 1. Remove timestamps older than the window
redis.call("ZREMRANGEBYSCORE", key, "-inf", window_start)

-- 2. Count remaining requests
local current_count = redis.call("ZCARD", key)

-- 3. Check if allowed
if current_count + cost > limit then
    -- Rejected. Calculate retryAfter based on the oldest element.
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    local ttl = 0
    if #oldest > 0 then
        local oldest_score = tonumber(oldest[2])
        ttl = oldest_score + window_ms - now_ms
        if ttl < 0 then ttl = 0 end
    else
        ttl = window_ms
    end
    return {0, limit - current_count, ttl}
end

-- 4. Allowed. Add the new requests.
for i = 1, cost do
    redis.call("ZADD", key, now_ms, base_id .. "-" .. tostring(i))
end

-- Set TTL for the entire key to clean up inactive keys
redis.call("PEXPIRE", key, window_ms)

return {1, limit - (current_count + cost), 0}
