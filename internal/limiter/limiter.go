package limiter

import (
	"context"
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
