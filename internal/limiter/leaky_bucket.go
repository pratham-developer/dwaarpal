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
	rc *redis.Client
}

// NewLeakyBucketLimiter creates a new LeakyBucketLimiter.
func NewLeakyBucketLimiter(rc *redis.Client) *LeakyBucketLimiter {
	return &LeakyBucketLimiter{
		rc: rc,
	}
}

// Queue adds the evaluation command to the pipeline.
func (l *LeakyBucketLimiter) Queue(ctx context.Context, pipe go_redis.Pipeliner, key string, limit int, window time.Duration, cost int) *go_redis.Cmd {
	windowMs := int(window.Milliseconds())
	if windowMs <= 0 {
		windowMs = 1000 // default to 1s if invalid
	}
	nowMs := time.Now().UnixMilli()
	return pipe.EvalSha(ctx, scripts.LeakyBucketSHA, []string{key}, limit, windowMs, nowMs, cost)
}

// Parse extracts the result from the executed pipeline command.
func (l *LeakyBucketLimiter) Parse(cmd *go_redis.Cmd) (Result, error) {
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
