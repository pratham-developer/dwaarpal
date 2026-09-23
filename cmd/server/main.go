package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/prathamkhanduja/dwaarpal/internal/config"
	"github.com/prathamkhanduja/dwaarpal/internal/handler"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Initialize Redis client
	rc, err := redis.NewClient(cfg.RedisAddress)
	if err != nil {
		log.Fatalf("Failed to initialize Redis client: %v", err)
	}
	defer rc.Close()

	log.Printf("Connected to Redis at %s", cfg.RedisAddress)

	// Initialize Handlers
	h := handler.NewHandler(rc)

	// Setup Routes
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.HealthCheck)
	mux.HandleFunc("/v1/check", h.CheckRateLimit)

	// Start HTTP Server
	serverAddr := fmt.Sprintf(":%s", cfg.Port)
	log.Printf("Starting HTTP server on %s", serverAddr)
	if err := http.ListenAndServe(serverAddr, mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
