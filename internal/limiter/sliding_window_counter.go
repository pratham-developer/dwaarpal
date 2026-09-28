package limiter

import (
	"context"
	_ "embed"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/redis"
	"github.com/prathamkhanduja/dwaarpal/internal/redis/scripts"
	go_redis "github.com/redis/go-redis/v9"
)

// SlidingWindowCounterLimiter implements the RateLimiter interface using a sliding window counter approximation.
type SlidingWindowCounterLimiter struct {
	rc *redis.Client
}

// NewSlidingWindowCounterLimiter creates a new SlidingWindowCounterLimiter.
func NewSlidingWindowCounterLimiter(rc *redis.Client) *SlidingWindowCounterLimiter {
	return &SlidingWindowCounterLimiter{
		rc: rc,
	}
}

// Queue adds the evaluation command to the pipeline.
func (l *SlidingWindowCounterLimiter) Queue(ctx context.Context, pipe go_redis.Pipeliner, key string, limit int, window time.Duration, cost int) *go_redis.Cmd {
	windowMs := int(window.Milliseconds())
	if windowMs <= 0 {
		windowMs = 1000 // Ensure at least 1 second
	}
	return pipe.EvalSha(ctx, scripts.SlidingWindowCounterSHA, []string{key}, limit, windowMs, cost)
}

// Parse extracts the result from the executed pipeline command.
func (l *SlidingWindowCounterLimiter) Parse(cmd *go_redis.Cmd) (Result, error) {
	return ParseLuaResult(cmd)
}
