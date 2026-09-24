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

// FixedWindowLimiter implements the RateLimiter interface using a fixed window algorithm.
type FixedWindowLimiter struct {
	rc *redis.Client
}

// NewFixedWindowLimiter creates a new FixedWindowLimiter.
func NewFixedWindowLimiter(rc *redis.Client) *FixedWindowLimiter {
	return &FixedWindowLimiter{
		rc: rc,
	}
}

// Queue adds the evaluation command to the pipeline.
func (l *FixedWindowLimiter) Queue(ctx context.Context, pipe go_redis.Pipeliner, key string, limit int, window time.Duration, cost int) *go_redis.Cmd {
	windowSeconds := int(window.Seconds())
	if windowSeconds <= 0 {
		windowSeconds = 1 // Ensure at least 1 second
	}
	return pipe.EvalSha(ctx, scripts.FixedWindowSHA, []string{key}, limit, windowSeconds, cost)
}

// Parse extracts the result from the executed pipeline command.
func (l *FixedWindowLimiter) Parse(cmd *go_redis.Cmd) (Result, error) {
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

	// Clean up fields based on whether the request was allowed
	if allowed {
		retryAfter = 0 // No retry delay if allowed
	} else {
		remaining = 0 // 0 remaining if rejected
	}

	return Result{
		Allowed:    allowed,
		Remaining:  remaining,
		RetryAfter: retryAfter,
		ResetAt:    resetAt,
	}, nil
}
