package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

func TestFixedWindowLimiter(t *testing.T) {
	rc, err := redis.NewClient("localhost:6379")
	if err != nil {
		t.Skipf("Skipping integration test, Redis not available at localhost:6379: %v", err)
	}
	defer rc.Close()

	ctx := context.Background()
	rateLimiter := limiter.NewFixedWindowLimiter(rc)

	key := "test:fixed_window:user123"
	limit := 3
	window := 2 * time.Second

	// Clean up before test
	rc.Rdb.Del(ctx, key)

	// Test allowing up to the limit
	for i := 1; i <= limit; i++ {
		res, err := allowHelper(rc, rateLimiter, ctx, key, limit, window, 1)
		if err != nil {
			t.Fatalf("Unexpected error on request %d: %v", i, err)
		}
		if !res.Allowed {
			t.Errorf("Request %d should have been allowed", i)
		}
		if res.Remaining != limit-i {
			t.Errorf("Expected remaining %d, got %d", limit-i, res.Remaining)
		}
	}

	// Test rejecting after limit
	res, err := allowHelper(rc, rateLimiter, ctx, key, limit, window, 1)
	if err != nil {
		t.Fatalf("Unexpected error on rejected request: %v", err)
	}
	if res.Allowed {
		t.Errorf("Request should have been rejected")
	}
	if res.Remaining != 0 {
		t.Errorf("Expected remaining 0 on reject, got %d", res.Remaining)
	}
	if res.RetryAfter <= 0 {
		t.Errorf("Expected positive RetryAfter, got %v", res.RetryAfter)
	}

	// Wait for window to expire (add a small buffer for Redis timing)
	time.Sleep(window + 100*time.Millisecond)

	// Should be allowed again
	res, err = allowHelper(rc, rateLimiter, ctx, key, limit, window, 1)
	if err != nil {
		t.Fatalf("Unexpected error after window reset: %v", err)
	}
	if !res.Allowed {
		t.Errorf("Request should have been allowed after window reset")
	}
}
