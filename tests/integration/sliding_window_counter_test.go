package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

func TestSlidingWindowCounterLimiter(t *testing.T) {
	rc, err := redis.NewClient("localhost:6379")
	if err != nil {
		t.Skipf("Skipping integration test, Redis not available: %v", err)
	}
	defer rc.Close()

	ctx := context.Background()
	rateLimiter := limiter.NewSlidingWindowCounterLimiter(rc)
	
	key := "test:sliding_window_counter:user123"
	limit := 5
	window := 2 * time.Second

	rc.Rdb.Del(ctx, key)

	// Fire 3 requests in the current window
	for i := 1; i <= 3; i++ {
		res, err := rateLimiter.Allow(ctx, key, limit, window, 1)
		if err != nil {
			t.Fatalf("Unexpected error on request %d: %v", i, err)
		}
		if !res.Allowed {
			t.Errorf("Request %d should have been allowed", i)
		}
	}

	// Move to the next window
	time.Sleep(2 * time.Second)

	// Fire 2 requests in the new window
	for i := 4; i <= 5; i++ {
		res, err := rateLimiter.Allow(ctx, key, limit, window, 1)
		if err != nil {
			t.Fatalf("Unexpected error on request %d: %v", i, err)
		}
		if !res.Allowed {
			t.Errorf("Request %d should have been allowed", i)
		}
	}

	// At this exact moment, elapsed time in the new window is roughly 0.
	// So weight of previous window (3 requests) is ~100%.
	// Current window has 2 requests.
	// Estimated requests = 3 + 2 = 5.
	// The limit is 5.
	// The next request should be rejected.
	res, err := rateLimiter.Allow(ctx, key, limit, window, 1)
	if err != nil || res.Allowed {
		t.Errorf("Request 6 should have been rejected (exceeded approximated limit)")
	}
	if res.RetryAfter <= 0 {
		t.Errorf("Expected positive RetryAfter")
	}
}
