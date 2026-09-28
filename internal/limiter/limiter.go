package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Result represents the outcome of a rate-limit check.
type Result struct {
	Allowed    bool          `json:"allowed"`
	Remaining  int           `json:"remaining"`
	RetryAfter time.Duration `json:"retryAfter"` // time until retry is possible
	ResetAt    time.Time     `json:"resetAt"`    // exact time the window resets
}

// RateLimiter defines the interface for different rate-limiting algorithms to support pipelining.
type RateLimiter interface {
	Queue(ctx context.Context, pipe redis.Pipeliner, key string, limit int, window time.Duration, cost int) *redis.Cmd
	Parse(cmd *redis.Cmd) (Result, error)
}

// ParseLuaResult extracts the standard [Allowed, Remaining, TTL] array returned by all Dwaarpal Lua scripts.
func ParseLuaResult(cmd *redis.Cmd) (Result, error) {
	res, err := cmd.Result()
	if err != nil {
		return Result{}, fmt.Errorf("redis script execution failed: %w", err)
	}

	resultArr, ok := res.([]interface{})
	if !ok || len(resultArr) != 3 {
		return Result{}, fmt.Errorf("unexpected script result format: %v", res)
	}

	allowedInt := resultArr[0].(int64)
	remaining := int(resultArr[1].(int64))
	ttlMs := resultArr[2].(int64)

	allowed := allowedInt == 1
	retryAfter := time.Duration(ttlMs) * time.Millisecond
	resetAt := time.Now().Add(retryAfter)

	if allowed {
		retryAfter = 0
	} else {
		remaining = 0
	}

	return Result{
		Allowed:    allowed,
		Remaining:  remaining,
		RetryAfter: retryAfter,
		ResetAt:    resetAt,
	}, nil
}
