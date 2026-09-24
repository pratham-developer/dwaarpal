package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/redis"
	"github.com/prathamkhanduja/dwaarpal/internal/redis/scripts"
	go_redis "github.com/redis/go-redis/v9"
)

// LeakyBucketLimiter implements the RateLimiter interface using a leaky bucket algorithm.
type LeakyBucketLimiter struct {
	rc     *redis.Client
	script *go_redis.Script
}

// NewLeakyBucketLimiter creates a new LeakyBucketLimiter.
func NewLeakyBucketLimiter(rc *redis.Client) *LeakyBucketLimiter {
	return &LeakyBucketLimiter{
		rc:     rc,
		script: go_redis.NewScript(scripts.LeakyBucket),
	}
}

// Allow checks if the request is allowed based on the leaky bucket algorithm.
func (l *LeakyBucketLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration, cost int) (Result, error) {
	windowMs := int(window.Milliseconds())
	if windowMs <= 0 {
		windowMs = 1000 // default to 1s if invalid
	}

	nowMs := time.Now().UnixMilli()

	// Execute the Lua script
	res, err := l.script.Run(ctx, l.rc.Rdb, []string{key}, limit, windowMs, nowMs, cost).Result()
	if err != nil {
		return Result{}, fmt.Errorf("redis script execution failed: %w", err)
	}

	// The Lua script returns {allowed, remaining_capacity, ttl_ms}
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
