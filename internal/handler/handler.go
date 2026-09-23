package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

// Handler holds dependencies for the HTTP handlers.
type Handler struct {
	RedisClient *redis.Client
}

// NewHandler creates a new Handler.
func NewHandler(rc *redis.Client) *Handler {
	return &Handler{
		RedisClient: rc,
	}
}

// HealthCheck responds with the server and Redis connection status.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	redisStatus := "connected"
	if err := h.RedisClient.Rdb.Ping(r.Context()).Err(); err != nil {
		redisStatus = "disconnected"
	}

	response := map[string]string{
		"status": "ok",
		"redis":  redisStatus,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// CheckRateLimit is a dummy endpoint for Milestone 1.
func (h *Handler) CheckRateLimit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// This is a placeholder response.
	// In Milestone 2+, we will parse the request, call the RateLimiter, and return the actual result.
	response := map[string]interface{}{
		"allowed":    true,
		"remaining":  99,
		"limit":      100,
		"retryAfter": 0,
		"resetAt":    time.Now().Add(time.Minute).Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
