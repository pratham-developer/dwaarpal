//go:build ignore

package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		allowed uint64
		denied  uint64
		errors  uint64
		wg      sync.WaitGroup
	)

	limit := flag.Int("limit", 100, "Quota limit")
	numRequests := flag.Int("concurrency", 2000, "Number of concurrent requests")
	apiURL := flag.String("url", "http://localhost/v1/check", "Target URL")
	flag.Parse()

	// Create a unique IP string for this correctness test
	testIP := fmt.Sprintf("race_test_ip_%d", time.Now().UnixNano())
	payload := fmt.Appendf(nil, `[{"key":"%s","algorithm":"FIXED_WINDOW","limit":%d,"window":60}]`, testIP, *limit)

	fmt.Printf("Starting Concurrency Correctness Test: %d Requests, Quota=%d, Target=%s\n", *numRequests, *limit, *apiURL)
	
	var readyWg sync.WaitGroup
	readyWg.Add(1)

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100000,
			MaxIdleConnsPerHost: 100000,
		},
	}

	for i := 0; i < *numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readyWg.Wait()

			req, _ := http.NewRequest("POST", *apiURL, bytes.NewBuffer(payload))
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				atomic.AddUint64(&errors, 1)
				return
			}
			defer resp.Body.Close()

			switch resp.StatusCode {
			case 200:
				atomic.AddUint64(&allowed, 1)
			case 429:
				atomic.AddUint64(&denied, 1)
			default:
				atomic.AddUint64(&errors, 1)
			}
		}()
	}

	// FIRE
	startTime := time.Now()
	readyWg.Done() 
	wg.Wait()
	duration := time.Since(startTime)

	fmt.Printf("\n=== RACE TEST RESULTS ===\n")
	fmt.Printf("Time Taken: %v\n", duration)
	fmt.Printf("Expected Allowed: %d\n", *limit)
	fmt.Printf("Actual Allowed: %d\n", allowed)
	fmt.Printf("Expected Denied: %d\n", *numRequests - *limit)
	fmt.Printf("Actual Denied: %d\n", denied)
	fmt.Printf("Errors (Timeouts/Fails): %d\n", errors)

	if allowed == uint64(*limit) && errors == 0 {
		fmt.Printf("\n✅ CORRECTNESS VERIFIED. The global quota was strictly enforced despite %d concurrent races.\n", *numRequests)
		os.Exit(0)
	} else if allowed <= uint64(*limit) {
		fmt.Printf("\n⚠️ PARTIAL SUCCESS. Under quota, but had %d errors.\n", errors)
		os.Exit(1)
	} else {
		fmt.Printf("\n❌ CORRECTNESS FAILED. Allowed %d > Limit %d\n", allowed, *limit)
		os.Exit(2)
	}
}
