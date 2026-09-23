package config

import (
	"os"
)

// Config holds the application configuration.
type Config struct {
	Port         string
	RedisAddress string
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

	return Config{
		Port:         port,
		RedisAddress: redisAddr,
	}
}
