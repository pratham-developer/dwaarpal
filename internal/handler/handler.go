package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/metrics"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

// Handler holds dependencies for the HTTP handlers.
type Handler struct {
	RedisClient *redis.Client
	Limiters    map[string]limiter.RateLimiter
	Timeout     time.Duration
	L1Cache     *lru.Cache[string, time.Time]
}

// NewHandler creates a new Handler.
func NewHandler(rc *redis.Client, limiters map[string]limiter.RateLimiter, timeout time.Duration, l1CacheSize int) *Handler {
	cache, _ := lru.New[string, time.Time](l1CacheSize)
	return &Handler{
		RedisClient: rc,
		Limiters:    limiters,
		Timeout:     timeout,
		L1Cache:     cache,
	}
}

// HealthCheck responds with the server and Redis connection status.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
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

	// L1 Cache (Penalty Box) Check
	l1Key := fmt.Sprintf("%s:%s:%d:%d", req.Algorithm, req.Key, req.Limit, req.Window)
	now := time.Now().UTC()

	if resetAt, ok := h.L1Cache.Get(l1Key); ok {
		if now.Before(resetAt) {
			// Cache hit! Still in penalty box. Block instantly.
			retryAfter := resetAt.Sub(now)

			slog.Info("L1 Cache Hit: Request blocked locally", "key", l1Key, "retryAfter", retryAfter)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(limiter.Result{
				Allowed:    false,
				Remaining:  0,
				RetryAfter: retryAfter,
				ResetAt:    resetAt,
			})
			return
		}
		// Penalty expired, naturally evict logic is handled by just proceeding.
	}

	start := time.Now()

	ctx, cancel := context.WithTimeout(r.Context(), h.Timeout)
	defer cancel()

	// Call the rate limiter (cost is fixed at 1 for now)
	res, err := limiterToUse.Allow(ctx, req.Key, req.Limit, windowDuration, 1)

	metrics.DecisionLatency.WithLabelValues(req.Algorithm).Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.RedisErrorsTotal.WithLabelValues(req.Algorithm).Inc()
		slog.Error("Rate limit decision failed, failing closed", "error", err, "key", req.Key, "algorithm", req.Algorithm)

		w.Header().Set("Content-Type", "application/json")
		// We return 503 Service Unavailable to indicate our dependency (Redis) failed,
		// but the payload format matches a rate limit rejection (Fail Closed).
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(limiter.Result{
			Allowed:    false,
			Remaining:  0,
			RetryAfter: 0,
		})
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

	// L1 Cache Population (Negative Caching)
	if !res.Allowed {
		// Enforce UTC for the ResetAt
		res.ResetAt = time.Now().UTC().Add(res.RetryAfter)
		h.L1Cache.Add(l1Key, res.ResetAt)
	} else {
		// For allowed requests, calculate a standard UTC ResetAt
		res.ResetAt = time.Now().UTC().Add(res.RetryAfter)
	}

	w.Header().Set("Content-Type", "application/json")
	if !res.Allowed {
		w.WriteHeader(http.StatusTooManyRequests)
	}
	json.NewEncoder(w).Encode(res)
}
