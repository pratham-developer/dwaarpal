package limiter

import (
	"context"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/redis"
	"github.com/prathamkhanduja/dwaarpal/internal/redis/scripts"
	go_redis "github.com/redis/go-redis/v9"
)

// TokenBucketLimiter implements the RateLimiter interface using a token bucket algorithm.
type TokenBucketLimiter struct {
	rc *redis.Client
}

// NewTokenBucketLimiter creates a new TokenBucketLimiter.
func NewTokenBucketLimiter(rc *redis.Client) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		rc: rc,
	}
}

// Queue adds the evaluation command to the pipeline.
func (l *TokenBucketLimiter) Queue(ctx context.Context, pipe go_redis.Pipeliner, key string, limit int, window time.Duration, cost int) *go_redis.Cmd {
	windowMs := int(window.Milliseconds())
	if windowMs <= 0 {
		windowMs = 1000 // default to 1s
	}
	return pipe.EvalSha(ctx, scripts.TokenBucketSHA, []string{key}, limit, windowMs, cost)
}

// Parse extracts the result from the executed pipeline command.
func (l *TokenBucketLimiter) Parse(cmd *go_redis.Cmd) (Result, error) {
	return ParseLuaResult(cmd)
}
