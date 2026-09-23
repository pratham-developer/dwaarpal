-- KEYS[1]: The rate limit key
-- ARGV[1]: capacity
-- ARGV[2]: window in milliseconds
-- ARGV[3]: current timestamp in milliseconds
-- ARGV[4]: request cost

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local refill_rate = capacity / window_ms

local tokens_key = "tokens"
local last_refill_key = "last_refill"

-- Read current state
local state = redis.call("HMGET", key, tokens_key, last_refill_key)
local tokens = tonumber(state[1])
local last_refill = tonumber(state[2])

if not tokens or not last_refill then
    tokens = capacity
    last_refill = now_ms
else
    local elapsed_ms = math.max(0, now_ms - last_refill)
    local new_tokens = elapsed_ms * refill_rate
    tokens = math.min(capacity, tokens + new_tokens)
    last_refill = now_ms
end

if tokens >= cost then
    -- Allowed
    tokens = tokens - cost
    redis.call("HSET", key, tokens_key, tokens, last_refill_key, last_refill)
    -- Extend TTL to the window duration to clean up inactive users
    redis.call("PEXPIRE", key, window_ms)
    return {1, math.floor(tokens), 0}
else
    -- Rejected
    -- Calculate retryAfter: ms needed to refill 'cost - tokens' tokens
    local tokens_needed = cost - tokens
    local ms_needed = math.ceil(tokens_needed / refill_rate)
    
    return {0, math.floor(tokens), ms_needed}
end
