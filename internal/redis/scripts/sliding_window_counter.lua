-- KEYS[1]: The rate limit key
-- ARGV[1]: limit
-- ARGV[2]: window in milliseconds
-- ARGV[3]: current timestamp in milliseconds
-- ARGV[4]: request cost

local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

-- Calculate the absolute start boundary of the current window
local curr_window_start = now_ms - (now_ms % window_ms)
local prev_window_start = curr_window_start - window_ms

local curr_window_start_str = tostring(curr_window_start)
local prev_window_start_str = tostring(prev_window_start)

local curr_count = 0
local prev_count = 0

-- Retrieve existing hash fields
local data = redis.call("HMGET", key, "curr_start", "curr_count", "prev_count")
local db_curr_start = data[1]
local db_curr_count = tonumber(data[2]) or 0
local db_prev_count = tonumber(data[3]) or 0

if db_curr_start == curr_window_start_str then
    -- We are still in the same active window
    curr_count = db_curr_count
    prev_count = db_prev_count
elseif db_curr_start == prev_window_start_str then
    -- The previous "current" window shifted and is now the "prev" window
    curr_count = 0
    prev_count = db_curr_count
else
    -- Much time has passed, the keys are completely expired/reset
    curr_count = 0
    prev_count = 0
end

-- Calculate overlap weight (percentage of previous window that overlaps with the lookback window)
local elapsed = now_ms - curr_window_start
local weight = (window_ms - elapsed) / window_ms

local estimated_requests = (prev_count * weight) + curr_count

if estimated_requests + cost > limit then
    -- Rejected. Simple TTL calculation: wait until the current window ends.
    local retry_after = window_ms - elapsed
    return {0, math.floor(limit - estimated_requests), retry_after}
end

-- Allowed. Increment the current counter.
curr_count = curr_count + cost
redis.call("HMSET", key, "curr_start", curr_window_start_str, "curr_count", curr_count, "prev_count", prev_count)
-- TTL for the hash should be 2x window (since we need prev_count data for the next window interpolation)
redis.call("PEXPIRE", key, window_ms * 2)

return {1, math.floor(limit - (estimated_requests + cost)), 0}
