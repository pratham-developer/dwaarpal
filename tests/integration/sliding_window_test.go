package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

func TestSlidingWindowLimiter(t *testing.T) {
	rc, err := redis.NewClient("localhost:6379")
	if err != nil {
		t.Skipf("Skipping integration test, Redis not available: %v", err)
	}
	defer rc.Close()

	ctx := context.Background()
	rateLimiter := limiter.NewSlidingWindowLimiter(rc)
	
	key := "test:sliding_window:user123"
	limit := 3
	window := 2 * time.Second

	rc.Rdb.Del(ctx, key)

	// Fire 2 requests initially
	for i := 1; i <= 2; i++ {
		res, err := rateLimiter.Allow(ctx, key, limit, window, 1)
		if err != nil {
			t.Fatalf("Unexpected error on request %d: %v", i, err)
		}
		if !res.Allowed {
			t.Errorf("Request %d should have been allowed", i)
		}
	}

	// Wait 1 second (half the window)
	time.Sleep(1 * time.Second)

	// Fire 2 more requests
	// First one should succeed (total 3 in last 2 seconds)
	res, err := rateLimiter.Allow(ctx, key, limit, window, 1)
	if err != nil || !res.Allowed {
		t.Errorf("Request 3 should have been allowed")
	}

	// Second one should fail (would be 4th in last 2 seconds)
	res, err = rateLimiter.Allow(ctx, key, limit, window, 1)
	if err != nil || res.Allowed {
		t.Errorf("Request 4 should have been rejected")
	}
	if res.RetryAfter <= 0 {
		t.Errorf("Expected positive RetryAfter")
	}

	// Wait for the first two requests to slide out of the window (another 1.1s)
	time.Sleep(1100 * time.Millisecond)

	// Now we should have 1 active request in the window (the one at t=1s).
	// Therefore, we should have 2 slots available.
	res, err = rateLimiter.Allow(ctx, key, limit, window, 1)
	if err != nil || !res.Allowed {
		t.Errorf("Request should have been allowed after first batch slid out")
	}
	if res.Remaining != 1 {
		t.Errorf("Expected remaining 1, got %d", res.Remaining)
	}
}
