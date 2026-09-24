package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

func TestLeakyBucketLimiter(t *testing.T) {
	rc, err := redis.NewClient("localhost:6379")
	if err != nil {
		t.Skipf("Skipping integration test, Redis not available: %v", err)
	}
	defer rc.Close()

	ctx := context.Background()
	rateLimiter := limiter.NewLeakyBucketLimiter(rc)

	key := "test:leaky_bucket:user123"
	// Bucket holds 5 units, takes 5 seconds to drain (leak rate = 1 unit per second)
	limit := 5
	window := 5 * time.Second

	rc.Rdb.Del(ctx, key)

	// Fire 5 requests instantly. They should all be allowed as they fill the bucket.
	for i := 1; i <= 5; i++ {
		res, err := rateLimiter.Allow(ctx, key, limit, window, 1)
		if err != nil {
			t.Fatalf("Unexpected error on request %d: %v", i, err)
		}
		if !res.Allowed {
			t.Errorf("Request %d should have been allowed", i)
		}
	}

	// 6th request immediately should fail since the bucket is completely full.
	res, err := rateLimiter.Allow(ctx, key, limit, window, 1)
	if err != nil || res.Allowed {
		t.Errorf("Request 6 should have been rejected (bucket full)")
	}
	if res.RetryAfter <= 0 {
		t.Errorf("Expected positive RetryAfter")
	}

	// Wait for 1.1 seconds. Exactly 1 unit of water should have leaked out by now.
	time.Sleep(1100 * time.Millisecond)

	// Now we can fire 1 more request.
	res, err = rateLimiter.Allow(ctx, key, limit, window, 1)
	if err != nil || !res.Allowed {
		t.Errorf("Request 7 should have been allowed after bucket leaked 1 unit")
	}

	// 8th request immediately should fail again because the bucket is full again.
	res, err = rateLimiter.Allow(ctx, key, limit, window, 1)
	if err != nil || res.Allowed {
		t.Errorf("Request 8 should have been rejected (bucket full again)")
	}
}
