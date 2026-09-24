package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
		"FIXED_WINDOW":           limiter.NewFixedWindowLimiter(rc),
		"SLIDING_WINDOW_LOG":     limiter.NewSlidingWindowLogLimiter(rc),
		"SLIDING_WINDOW_COUNTER": limiter.NewSlidingWindowCounterLimiter(rc),
		"TOKEN_BUCKET":           limiter.NewTokenBucketLimiter(rc),
		"LEAKY_BUCKET":           limiter.NewLeakyBucketLimiter(rc),
	}

	// Initialize Handlers
	h := handler.NewHandler(rc, limiters, cfg.RedisTimeout)

	// Setup Routes
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.HealthCheck)
	mux.HandleFunc("POST /v1/check", h.CheckRateLimit)
	mux.Handle("GET /metrics", promhttp.Handler())

	// Setup gRPC Server
	grpcServer := grpc.NewServer()
	proto.RegisterRateLimiterServiceServer(grpcServer, grpc_handler.NewServer(limiters, cfg.RedisTimeout))
	reflection.Register(grpcServer)

	// Start gRPC Server
	go func() {
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("failed to listen on :50051: %v", err)
		}
		log.Printf("Starting gRPC server on :50051")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// Setup HTTP Server
	serverAddr := fmt.Sprintf(":%s", cfg.Port)
	httpServer := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}

	// Start HTTP Server
	go func() {
		log.Printf("Starting HTTP server on %s", serverAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Graceful Shutdown Channel
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Block until signal is received
	sig := <-quit
	log.Printf("Received signal: %v. Initiating graceful shutdown...", sig)

	// Context with 15-second timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Stop HTTP Server
	log.Println("Stopping HTTP server...")
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP server forced to shutdown: %v", err)
	}

	// 2. Stop gRPC Server
	log.Println("Stopping gRPC server...")
	grpcServer.GracefulStop()

	// 3. (Deferred) Redis connection pool will close when main exits
	log.Println("Dwaarpal shutdown complete.")
}
