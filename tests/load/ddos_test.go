package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type DDoSCheckRequest struct {
	Key       string `json:"key"`
	Algorithm string `json:"algorithm"`
	Limit     int    `json:"limit"`
	Window    int    `json:"window"`
}

func TestDDoS(t *testing.T) {
	t.Skip("Run this manually while the server is running on :8080")
}

// To run: go test -v -run TestDDoS_Active ./tests/load
func TestDDoS_Active(t *testing.T) {
	// We will simulate 10 different malicious IPs (keys)
	numKeys := 10
	// Each IP will send 500 requests as fast as possible
	requestsPerKey := 500
	limit := 5

	var successCount int32
	var rejectedCount int32
	var errorCount int32

	var wg sync.WaitGroup

	startTime := time.Now()

	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("attacker_ip_%d", k)

		// 1. Sequentially breach the limit to safely populate the L1 Cache without crashing the Upstash connection pool
		for i := 0; i <= limit; i++ {
			reqBody := []DDoSCheckRequest{{Key: key, Algorithm: "TOKEN_BUCKET", Limit: limit, Window: 60}}
			jsonData, _ := json.Marshal(reqBody)
			http.Post("http://localhost:8080/v1/check", "application/json", bytes.NewBuffer(jsonData))
		}

		time.Sleep(100 * time.Millisecond) // Let the cache settle

		// 2. Now launch the massive parallel DDoS against the L1 Cache!
		for r := 0; r < requestsPerKey; r++ {
			wg.Add(1)
			go func(ip string) {
				defer wg.Done()

				reqBody := []DDoSCheckRequest{{
					Key:       ip,
					Algorithm: "TOKEN_BUCKET",
					Limit:     limit,
					Window:    60,
				}}
				jsonData, _ := json.Marshal(reqBody)

				resp, err := http.Post("http://localhost:8080/v1/check", "application/json", bytes.NewBuffer(jsonData))
				if err != nil {
					atomic.AddInt32(&errorCount, 1)
					return
				}
				defer resp.Body.Close()

				switch resp.StatusCode {
				case http.StatusOK:
					atomic.AddInt32(&successCount, 1)
				case http.StatusTooManyRequests:
					atomic.AddInt32(&rejectedCount, 1)
				default:
					atomic.AddInt32(&errorCount, 1)
				}
			}(key)
		}
	}

	wg.Wait()
	duration := time.Since(startTime)

	t.Logf("--- DDoS Simulation Results ---")
	t.Logf("Total Requests Sent: %d", numKeys*requestsPerKey)
	t.Logf("Time Taken: %v", duration)
	t.Logf("Requests/sec: %.2f", float64(numKeys*requestsPerKey)/duration.Seconds())
	t.Logf("Allowed (200 OK): %d (Expected: ~%d)", successCount, numKeys*limit)
	t.Logf("Blocked (429 Too Many Requests): %d", rejectedCount)
	t.Logf("Errors: %d", errorCount)
	t.Logf("-------------------------------")
	t.Logf("Check the Dwaarpal server terminal logs. You should see massive bursts of 'L1 Cache Hit' bypassing Redis entirely!")
}
