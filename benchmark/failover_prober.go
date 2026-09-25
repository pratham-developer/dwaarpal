//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"time"
)

// The prober constantly fires requests and logs exact ms timestamps
// of state transitions (200 OK vs 500 Error).
func main() {
	apiURL := "http://localhost:8080/v1/check"
	payload := []byte(`[{"key":"prober_key","algorithm":"FIXED_WINDOW","limit":1000000,"window":60}]`)

	client := &http.Client{
		Timeout: 500 * time.Millisecond,
	}

	fmt.Printf("Prober started. Target: %s\n", apiURL)
	
	// Wait for API to be initially up
	for {
		req, _ := http.NewRequest("POST", apiURL, bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("T0 (Baseline): API is healthy. 200 OK.\n")
	
	// Open a file to append raw results for analysis
	f, err := os.OpenFile("/tmp/dwaarpal_failover_log.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	lastState := 200
	stateChangeTime := time.Now()

	for {
		req, _ := http.NewRequest("POST", apiURL, bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")
		
		reqStart := time.Now()
		resp, err := client.Do(req)
		
		currentState := 0
		if err != nil {
			currentState = 500 // Treat timeout/conn-refused as 500
		} else {
			currentState = resp.StatusCode
			resp.Body.Close()
		}

		if currentState != lastState {
			msg := fmt.Sprintf("[%s] State changed from %d -> %d (+%d ms since last change)\n", 
				time.Now().Format("15:04:05.000"), lastState, currentState, time.Since(stateChangeTime).Milliseconds())
			fmt.Print(msg)
			f.WriteString(msg)
			lastState = currentState
			stateChangeTime = time.Now()
		}

		// Adjust sleep to maintain ~50 RPS probing
		elapsed := time.Since(reqStart)
		sleepDur := (20 * time.Millisecond) - elapsed
		if sleepDur > 0 {
			time.Sleep(sleepDur)
		}
	}
}
