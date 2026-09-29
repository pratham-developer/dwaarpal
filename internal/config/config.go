package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the application configuration.
type Config struct {
	Port          string
	GrpcPort      string
	RedisAddress  string
	RedisTimeout  time.Duration
	L1CacheSize   int
	RedisPoolSize int
	MaxBatchSize  int
	FailOpen      bool
}

// LoadConfig loads configuration from environment variables with sensible defaults.
func LoadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	redisAddr := os.Getenv("REDIS_ADDRESS")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	timeoutStr := os.Getenv("REDIS_TIMEOUT_MS")
	timeoutMs := 50
	if timeoutStr != "" {
		if parsed, err := strconv.Atoi(timeoutStr); err == nil && parsed > 0 {
			timeoutMs = parsed
		}
	}

	l1CacheStr := os.Getenv("L1_CACHE_SIZE")
	l1CacheSize := 100000 // default 100k keys
	if l1CacheStr != "" {
		if parsed, err := strconv.Atoi(l1CacheStr); err == nil && parsed > 0 {
			l1CacheSize = parsed
		}
	}

	poolSizeStr := os.Getenv("REDIS_POOL_SIZE")
	redisPoolSize := 0 // default falls back to go-redis (10 * runtime.GOMAXPROCS)
	if poolSizeStr != "" {
		if parsed, err := strconv.Atoi(poolSizeStr); err == nil && parsed > 0 {
			redisPoolSize = parsed
		}
	}

	batchSizeStr := os.Getenv("MAX_BATCH_SIZE")
	maxBatchSize := 100 // default 100 keys per request
	if batchSizeStr != "" {
		if parsed, err := strconv.Atoi(batchSizeStr); err == nil && parsed > 0 {
			maxBatchSize = parsed
		}
	}

	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	failOpen := os.Getenv("FAIL_OPEN") == "true"

	return Config{
		Port:          port,
		GrpcPort:      grpcPort,
		RedisAddress:  redisAddr,
		RedisTimeout:  time.Duration(timeoutMs) * time.Millisecond,
		L1CacheSize:   l1CacheSize,
		RedisPoolSize: redisPoolSize,
		MaxBatchSize: maxBatchSize,
		FailOpen:     failOpen,
	}
}
