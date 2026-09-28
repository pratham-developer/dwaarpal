package limiter

import (
	"context"
	_ "embed"
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
	return pipe.EvalSha(ctx, scripts.LeakyBucketSHA, []string{key}, limit, windowMs, cost)
}

// Parse extracts the result from the executed pipeline command.
func (l *LeakyBucketLimiter) Parse(cmd *go_redis.Cmd) (Result, error) {
	return ParseLuaResult(cmd)
}
