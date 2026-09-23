package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

func TestTokenBucketLimiter(t *testing.T) {
	rc, err := redis.NewClient("localhost:6379")
	if err != nil {
		t.Skipf("Skipping integration test, Redis not available: %v", err)
	}
	defer rc.Close()

	ctx := context.Background()
	rateLimiter := limiter.NewTokenBucketLimiter(rc)
	
	key := "test:token_bucket:user123"
	// 5 tokens per 1 second = refill rate of 5 tokens/sec
	limit := 5
	window := 1 * time.Second

	rc.Rdb.Del(ctx, key)

	// Burst: Consume all 5 tokens
	for i := 1; i <= limit; i++ {
		res, err := rateLimiter.Allow(ctx, key, limit, window, 1)
		if err != nil {
			t.Fatalf("Unexpected error on request %d: %v", i, err)
		}
		if !res.Allowed {
			t.Errorf("Request %d should have been allowed", i)
		}
	}

	// Next request should fail immediately
	res, err := rateLimiter.Allow(ctx, key, limit, window, 1)
	if err != nil || res.Allowed {
		t.Errorf("Request after burst should have been rejected")
	}

	// Wait 400ms -> should refill 2 tokens (5 tokens/sec * 0.4s = 2 tokens)
	// We wait slightly longer to account for execution time
	time.Sleep(450 * time.Millisecond)

	// Should allow exactly 2 requests
	for i := 1; i <= 2; i++ {
		res, err = rateLimiter.Allow(ctx, key, limit, window, 1)
		if err != nil || !res.Allowed {
			t.Errorf("Refilled request %d should have been allowed", i)
		}
	}

	// 3rd request should fail
	res, err = rateLimiter.Allow(ctx, key, limit, window, 1)
	if err != nil || res.Allowed {
		t.Errorf("3rd refilled request should have been rejected")
	}
}
