package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/metrics"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

// Handler holds dependencies for the HTTP handlers.
type Handler struct {
	RedisClient *redis.Client
	Limiters    map[string]limiter.RateLimiter
}

// NewHandler creates a new Handler.
func NewHandler(rc *redis.Client, limiters map[string]limiter.RateLimiter) *Handler {
	return &Handler{
		RedisClient: rc,
		Limiters:    limiters,
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

// CheckRequest represents the JSON payload for a rate-limit check.
type CheckRequest struct {
	Key       string `json:"key"`
	Algorithm string `json:"algorithm"`
	Limit     int    `json:"limit"`
	Window    int    `json:"window"` // window in seconds
}

// CheckRateLimit handles rate-limiting decisions.
func (h *Handler) CheckRateLimit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Set sensible defaults if not provided
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Window <= 0 {
		req.Window = 60
	}
	if req.Key == "" {
		http.Error(w, "Missing 'key' in request", http.StatusBadRequest)
		return
	}
	if req.Algorithm == "" {
		req.Algorithm = "TOKEN_BUCKET"
	}

	limiterToUse, exists := h.Limiters[req.Algorithm]
	if !exists {
		http.Error(w, "Unsupported algorithm", http.StatusBadRequest)
		return
	}

	windowDuration := time.Duration(req.Window) * time.Second

	start := time.Now()

	// Call the rate limiter (cost is fixed at 1 for now)
	res, err := limiterToUse.Allow(r.Context(), req.Key, req.Limit, windowDuration, 1)
	
	metrics.DecisionLatency.WithLabelValues(req.Algorithm).Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.RedisErrorsTotal.WithLabelValues(req.Algorithm).Inc()
		slog.Error("Rate limit decision failed", "error", err, "key", req.Key, "algorithm", req.Algorithm)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	metrics.RequestsTotal.WithLabelValues(req.Algorithm, strconv.FormatBool(res.Allowed)).Inc()

	slog.Info("Rate limit decision",
		"key", req.Key,
		"algorithm", req.Algorithm,
		"allowed", res.Allowed,
		"remaining", res.Remaining,
		"retryAfter", res.RetryAfter,
	)

	w.Header().Set("Content-Type", "application/json")
	if !res.Allowed {
		w.WriteHeader(http.StatusTooManyRequests)
	}
	json.NewEncoder(w).Encode(res)
}
