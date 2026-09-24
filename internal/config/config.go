package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the application configuration.
type Config struct {
	Port         string
	RedisAddress string
	RedisTimeout time.Duration
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

	return Config{
		Port:         port,
		RedisAddress: redisAddr,
		RedisTimeout: time.Duration(timeoutMs) * time.Millisecond,
	}
}
