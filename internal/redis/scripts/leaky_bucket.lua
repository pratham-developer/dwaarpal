-- KEYS[1]: The rate limit key
-- ARGV[1]: limit (bucket capacity)
-- ARGV[2]: window in milliseconds (time it takes to completely drain a full bucket)
-- ARGV[3]: current timestamp in milliseconds
-- ARGV[4]: request cost

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

-- Leak rate: how many units of water leak per millisecond
local leak_rate = limit / window_ms

-- Retrieve current state: water level and last update timestamp
local data = redis.call("HMGET", key, "water", "last_update")
local water = tonumber(data[1]) or 0
local last_update = tonumber(data[2]) or now_ms

-- Calculate how much water has leaked since the last request
local elapsed = math.max(0, now_ms - last_update)
local leaked = elapsed * leak_rate

-- Update the water level in the bucket
water = math.max(0, water - leaked)

if water + cost > limit then
    -- The bucket overflows, reject the request.
    -- Calculate exactly how many milliseconds until enough water leaks to fit this cost.
    local required_leak = (water + cost) - limit
    local retry_after = math.ceil(required_leak / leak_rate)
    return {0, math.floor(limit - water), retry_after}
end

-- Allowed. Pour water into the bucket.
water = water + cost
redis.call("HMSET", key, "water", water, "last_update", now_ms)

-- The TTL is the exact time it takes for the bucket to completely drain to zero.
local drain_time = math.ceil(water / leak_rate)
redis.call("PEXPIRE", key, drain_time)

return {1, math.floor(limit - water), 0}
