package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/redis"
	"github.com/prathamkhanduja/dwaarpal/internal/redis/scripts"
	go_redis "github.com/redis/go-redis/v9"
)

// TokenBucketLimiter implements the RateLimiter interface using a token bucket algorithm.
type TokenBucketLimiter struct {
	rc     *redis.Client
	script *go_redis.Script
}

// NewTokenBucketLimiter creates a new TokenBucketLimiter.
func NewTokenBucketLimiter(rc *redis.Client) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		rc:     rc,
		script: go_redis.NewScript(scripts.TokenBucket),
	}
}

// Allow checks if the request is allowed based on the token bucket algorithm.
func (l *TokenBucketLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration, cost int) (Result, error) {
	windowMs := int(window.Milliseconds())
	if windowMs <= 0 {
		windowMs = 1000 // default to 1s
	}

	nowMs := time.Now().UnixMilli()

	// Execute the Lua script
	res, err := l.script.Run(ctx, l.rc.Rdb, []string{key}, limit, windowMs, nowMs, cost).Result()
	if err != nil {
		return Result{}, fmt.Errorf("redis script execution failed: %w", err)
	}

	// The Lua script returns {allowed, remaining, ttl_ms}
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
