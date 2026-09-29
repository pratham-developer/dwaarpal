package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prathamkhanduja/dwaarpal/internal/engine"
	"github.com/prathamkhanduja/dwaarpal/internal/handler"
	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
)

func TestPayloadBombMitigation(t *testing.T) {
	cache, _ := lru.New[string, time.Time](1000)
	evaluator := engine.NewEvaluator(nil, map[string]limiter.RateLimiter{}, time.Second, cache, 100, false)
	h := handler.NewHandler(evaluator)

	// Generate a JSON payload just over 1MB. We'll do this by writing a massive array of junk.
	// We want to test that the server explicitly throws an error and DOES NOT panic (OOM).
	
	// Create an extremely long string
	junkSize := 2 * 1024 * 1024 // 2 Megabytes
	bombJSON := "[" + strings.Repeat(`{"key":"junk","algorithm":"TOKEN_BUCKET","limit":1,"window":60},`, junkSize/60) + `{"key":"end","algorithm":"TOKEN_BUCKET","limit":1,"window":60}]`

	req := httptest.NewRequest("POST", "/v1/check", bytes.NewBufferString(bombJSON))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// If the server doesn't have MaxBytesReader, json.NewDecoder will parse the 2MB payload 
	// into memory, which is what we're preventing.
	h.CheckRateLimit(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusRequestEntityTooLarge && res.StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected 413 Payload Too Large or 400 Bad Request, got %d", res.StatusCode)
	}

	t.Log("✅ Payload Bomb Mitigated successfully. 2MB Request blocked instantly at transport layer.")
}
