package main

import (
	"fmt"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

func TestLRUCacheEviction(t *testing.T) {
	// 1. Create a very small cache (Max 5 keys)
	cacheSize := 5
	cache, err := lru.New[string, time.Time](cacheSize)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// 2. Insert 5 keys (Cache is now full)
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key_%d", i)
		cache.Add(key, time.Now().UTC())
	}

	if cache.Len() != 5 {
		t.Errorf("Expected cache length 5, got %d", cache.Len())
	}

	// 3. We insert a 6th key. Because it's an LRU, "key_1" (the oldest) MUST be evicted!
	cache.Add("key_6", time.Now().UTC())

	// 4. Verify length is still mathematically bounded to 5
	if cache.Len() != 5 {
		t.Errorf("Expected cache length 5, got %d. OOM PROTECTION FAILED!", cache.Len())
	}

	// 5. Verify the exact key that was evicted
	_, ok1 := cache.Get("key_1")
	if ok1 {
		t.Errorf("Expected key_1 to be evicted, but it was found in the cache!")
	}

	_, ok6 := cache.Get("key_6")
	if !ok6 {
		t.Errorf("Expected key_6 to be in the cache, but it was not found!")
	}

	t.Log("✅ LRU Eviction Test Passed. Memory is strictly bounded.")
}
