package limiter

import (
	"context"
	"time"
)

// Result represents the outcome of a rate-limit check.
type Result struct {
	Allowed    bool          `json:"allowed"`
	Remaining  int           `json:"remaining"`
	RetryAfter time.Duration `json:"retryAfter"` // time until retry is possible
	ResetAt    time.Time     `json:"resetAt"`    // exact time the window resets
}

// RateLimiter defines the interface for different rate-limiting algorithms.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration, cost int) (Result, error)
}
