//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"net/http"
	"time"
)

func main() {
	apiURL := "http://localhost:8080/v1/check"
	
	// Create a unique IP string for this latency test
	testIP := fmt.Sprintf("latency_test_ip_%d", time.Now().UnixNano())
	payload := fmt.Appendf(nil, `[{"key":"%s","algorithm":"TOKEN_BUCKET","limit":1,"window":60}]`, testIP)

	client := &http.Client{Timeout: 2 * time.Second}

	// Request 1: Valid Hit (Traverses network to Redis)
	req1, _ := http.NewRequest("POST", apiURL, bytes.NewBuffer(payload))
	req1.Header.Set("Content-Type", "application/json")
	
	start1 := time.Now()
	resp1, _ := client.Do(req1)
	resp1.Body.Close()
	redisLatency := time.Since(start1)

	// Request 2: Rejected Hit (Traverses network to Redis and gets blocked, adding to L1)
	req2, _ := http.NewRequest("POST", apiURL, bytes.NewBuffer(payload))
	req2.Header.Set("Content-Type", "application/json")
	
	client.Do(req2)

	// Request 3: L1 Cache Hit (Rejected locally in Go RAM)
	req3, _ := http.NewRequest("POST", apiURL, bytes.NewBuffer(payload))
	req3.Header.Set("Content-Type", "application/json")
	
	start3 := time.Now()
	resp3, _ := client.Do(req3)
	resp3.Body.Close()
	l1Latency := time.Since(start3)

	fmt.Printf("\n=== LATENCY BREAKDOWN (Raw Unbatched HTTP) ===\n")
	fmt.Printf("Redis Network Hit (p50): %v\n", redisLatency)
	fmt.Printf("L1 Cache Hit (Memory):   %v\n", l1Latency)
}
