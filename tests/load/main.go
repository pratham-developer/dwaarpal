package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

type CheckRequest struct {
	Key       string `json:"key"`
	Algorithm string `json:"algorithm"`
	Limit     int    `json:"limit"`
	Window    int    `json:"window"`
}

type CheckResponse struct {
	Allowed    bool `json:"allowed"`
	Remaining  int  `json:"remaining"`
	RetryAfter int  `json:"retryAfter"`
}

type Result struct {
	Duration time.Duration
	Status   int
	Allowed  bool
	Error    error
}

func main() {
	url := flag.String("url", "http://localhost:8080/v1/check", "URL to test")
	requests := flag.Int("requests", 10000, "Total number of requests")
	concurrency := flag.Int("concurrency", 500, "Number of concurrent workers")
	key := flag.String("key", "user:loadtest", "Rate limit key")
	algorithm := flag.String("algorithm", "TOKEN_BUCKET", "Algorithm to test")
	limit := flag.Int("limit", 100, "Rate limit capacity")
	window := flag.Int("window", 60, "Rate limit window in seconds")

	flag.Parse()

	fmt.Printf("Starting load test against %s\n", *url)
	fmt.Printf("Requests: %d, Concurrency: %d, Algorithm: %s\n", *requests, *concurrency, *algorithm)

	reqPayload := []CheckRequest{{
		Key:       *key,
		Algorithm: *algorithm,
		Limit:     *limit,
		Window:    *window,
	}}

	payloadBytes, _ := json.Marshal(reqPayload)

	jobs := make(chan struct{}, *requests)
	results := make(chan Result, *requests)

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Custom transport to handle high concurrency locally without running out of sockets
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.MaxIdleConns = *concurrency
			t.MaxIdleConnsPerHost = *concurrency

			client := &http.Client{
				Timeout:   10 * time.Second,
				Transport: t,
			}

			for range jobs {
				start := time.Now()
				req, _ := http.NewRequest(http.MethodPost, *url, bytes.NewBuffer(payloadBytes))
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				duration := time.Since(start)

				if err != nil {
					results <- Result{Duration: duration, Error: err}
					continue
				}

				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()

				var cr CheckResponse
				json.Unmarshal(body, &cr)

				results <- Result{
					Duration: duration,
					Status:   resp.StatusCode,
					Allowed:  cr.Allowed,
				}
			}
		}()
	}

	startTime := time.Now()

	// Dispatch jobs
	for i := 0; i < *requests; i++ {
		jobs <- struct{}{}
	}
	close(jobs)

	// Wait for workers
	wg.Wait()
	close(results)

	totalDuration := time.Since(startTime)

	// Process results
	var successCount, allowedCount, rejectedCount, errCount int
	var durations []time.Duration

	for res := range results {
		if res.Error != nil {
			errCount++
			continue
		}
		successCount++
		if res.Allowed {
			allowedCount++
		} else {
			rejectedCount++
		}
		durations = append(durations, res.Duration)
	}

	fmt.Printf("\n--- Results ---\n")
	fmt.Printf("Total time: %v\n", totalDuration)
	fmt.Printf("Total requests: %d\n", *requests)
	fmt.Printf("Successful requests: %d\n", successCount)
	fmt.Printf("Errors: %d\n", errCount)
	fmt.Printf("Allowed: %d\n", allowedCount)
	fmt.Printf("Rejected: %d\n", rejectedCount)

	if successCount > 0 {
		throughput := float64(successCount) / totalDuration.Seconds()
		fmt.Printf("Throughput: %.2f req/sec\n", throughput)

		sort.Slice(durations, func(i, j int) bool {
			return durations[i] < durations[j]
		})

		p50 := durations[len(durations)*50/100]
		p95 := durations[len(durations)*95/100]
		p99 := durations[len(durations)*99/100]

		fmt.Printf("\nLatency:\n")
		fmt.Printf("p50: %v\n", p50)
		fmt.Printf("p95: %v\n", p95)
		fmt.Printf("p99: %v\n", p99)
	}
}
