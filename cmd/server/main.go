package main

import (
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/prathamkhanduja/dwaarpal/api/proto"
	"github.com/prathamkhanduja/dwaarpal/internal/config"
	grpc_handler "github.com/prathamkhanduja/dwaarpal/internal/grpc"
	"github.com/prathamkhanduja/dwaarpal/internal/handler"
	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/metrics"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Initialize metrics
	metrics.Init()

	// Initialize Redis client
	rc, err := redis.NewClient(cfg.RedisAddress)
	if err != nil {
		log.Fatalf("Failed to initialize Redis client: %v", err)
	}
	defer rc.Close()

	log.Printf("Connected to Redis at %s", cfg.RedisAddress)

	// Initialize Rate Limiters
	limiters := map[string]limiter.RateLimiter{
		"FIXED_WINDOW":   limiter.NewFixedWindowLimiter(rc),
		"SLIDING_WINDOW": limiter.NewSlidingWindowLimiter(rc),
		"TOKEN_BUCKET":   limiter.NewTokenBucketLimiter(rc),
	}

	// Initialize Handlers
	h := handler.NewHandler(rc, limiters)

	// Setup Routes
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.HealthCheck)
	mux.HandleFunc("/v1/check", h.CheckRateLimit)
	mux.Handle("/metrics", promhttp.Handler())

	// Start gRPC Server
	go func() {
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("failed to listen on :50051: %v", err)
		}

		grpcServer := grpc.NewServer()
		proto.RegisterRateLimiterServiceServer(grpcServer, grpc_handler.NewServer(limiters))
		reflection.Register(grpcServer)

		log.Printf("Starting gRPC server on :50051")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// Start HTTP Server
	serverAddr := fmt.Sprintf(":%s", cfg.Port)
	log.Printf("Starting HTTP server on %s", serverAddr)
	if err := http.ListenAndServe(serverAddr, mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
