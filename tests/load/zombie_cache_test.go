package main

import (
	"bytes"
	"net/http/httptest"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prathamkhanduja/dwaarpal/internal/engine"
	"github.com/prathamkhanduja/dwaarpal/internal/handler"
	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
)

func TestZombieCacheEviction(t *testing.T) {
	// 1. Create a Handler with an artificially small L1 Cache size of 2
	cache, _ := lru.New[string, time.Time](2)
	evaluator := engine.NewEvaluator(nil, map[string]limiter.RateLimiter{}, time.Second, cache, 10, false)
	h := handler.NewHandler(evaluator)

	// 2. Pre-populate the cache with a "Zombie" key that expired 1 hour ago
	zombieKey := "TOKEN_BUCKET:ZOMBIE:1:60"
	cache.Add(zombieKey, time.Now().UTC().Add(-time.Hour)) // Expired in the past

	// 3. Populate a legitimate active blocked key
	activeKey := "TOKEN_BUCKET:LEGIT:1:60"
	cache.Add(activeKey, time.Now().UTC().Add(time.Hour)) // Expires in the future

	// 4. Verify cache is completely full (size = 2)
	if cache.Len() != 2 {
		t.Fatalf("Expected cache size 2, got %d", cache.Len())
	}

	// 5. Attacker tries to keep the Zombie key alive by trickling a request to it
	payload := `[{"key": "ZOMBIE", "algorithm": "TOKEN_BUCKET", "limit": 1, "window": 60}]`
	req := httptest.NewRequest("POST", "/v1/check", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// 6. Execute the handler logic
	// The handler's Phase A L1 Cache Check should detect the key, see it's expired, and PROACTIVELY EVICT IT.

	defer func() {
		recover() // Catch the expected panic from nil RedisClient reaching Phase B

		// 7. Core Assertion: The Zombie key MUST have been proactively evicted!
		if _, exists := cache.Get(zombieKey); exists {
			t.Fatalf("❌ VULNERABILITY DETECTED: Zombie key was NOT evicted! It is polluting the cache!")
		}

		// 8. Core Assertion: The Legitimate key MUST still be in the cache!
		if _, exists := cache.Get(activeKey); !exists {
			t.Fatalf("❌ VULNERABILITY DETECTED: Legitimate key was evicted because the cache filled up!")
		}

		t.Log("✅ Zombie Cache Vulnerability Mitigated: Expired key was proactively evicted upon read.")
	}()

	h.CheckRateLimit(w, req)
}
