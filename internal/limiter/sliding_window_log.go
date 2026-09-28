package limiter

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
	"github.com/prathamkhanduja/dwaarpal/internal/redis/scripts"
	go_redis "github.com/redis/go-redis/v9"
)

// SlidingWindowLogLimiter implements the RateLimiter interface using a sliding window log algorithm.
type SlidingWindowLogLimiter struct {
	rc *redis.Client
}

// NewSlidingWindowLogLimiter creates a new SlidingWindowLogLimiter.
func NewSlidingWindowLogLimiter(rc *redis.Client) *SlidingWindowLogLimiter {
	return &SlidingWindowLogLimiter{
		rc: rc,
	}
}

// Queue adds the evaluation command to the pipeline.
func (l *SlidingWindowLogLimiter) Queue(ctx context.Context, pipe go_redis.Pipeliner, key string, limit int, window time.Duration, cost int) *go_redis.Cmd {
	windowMs := int(window.Milliseconds())
	if windowMs <= 0 {
		windowMs = 1000 // default to 1s if invalid
	}

	baseID := uuid.New().String()

	return pipe.EvalSha(ctx, scripts.SlidingWindowLogSHA, []string{key}, limit, windowMs, cost, baseID)
}

// Parse extracts the result from the executed pipeline command.
func (l *SlidingWindowLogLimiter) Parse(cmd *go_redis.Cmd) (Result, error) {
	return ParseLuaResult(cmd)
}
