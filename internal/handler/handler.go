package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/engine"
)

// Handler holds dependencies for the HTTP handlers.
type Handler struct {
	Engine *engine.Evaluator
}

// NewHandler creates a new Handler.
func NewHandler(e *engine.Evaluator) *Handler {
	return &Handler{
		Engine: e,
	}
}

// HealthCheck responds with the server and Redis connection status.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	redisStatus := "connected"
	if err := h.Engine.RedisClient.Rdb.Ping(r.Context()).Err(); err != nil {
		redisStatus = "disconnected"
	}

	response := map[string]string{
		"status": "ok",
		"redis":  redisStatus,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// CheckMultiResponse represents the aggregated result.
type CheckMultiResponse struct {
	Allowed    bool   `json:"allowed"`
	FailedKey  string `json:"failed_key,omitempty"`
	Remaining  int    `json:"remaining"`
	RetryAfter int64  `json:"retry_after"` // milliseconds
	ResetAt    string `json:"reset_at"`
}

// CheckRateLimit handles multi-key rate-limiting decisions via the central engine.
func (h *Handler) CheckRateLimit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024) // 1 MB absolute hard cap

	var reqs []engine.Request
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		http.Error(w, "Invalid JSON payload, expected an array of descriptors or payload too large", http.StatusBadRequest)
		return
	}

	decision, err := h.Engine.EvaluateBatch(r.Context(), reqs)

	if err != nil {
		// Differentiate between bad request vs infra error
		if err.Error() == "empty requests array" || err.Error()[:10] == "batch size" || err.Error()[:7] == "missing" || err.Error()[:11] == "unsupported" {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(CheckMultiResponse{
			Allowed:   false,
			Remaining: 0,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if !decision.Allowed {
		w.WriteHeader(http.StatusTooManyRequests)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	json.NewEncoder(w).Encode(CheckMultiResponse{
		Allowed:    decision.Allowed,
		FailedKey:  decision.FailedKey,
		Remaining:  decision.Remaining,
		RetryAfter: decision.RetryAfter.Milliseconds(),
		ResetAt:    decision.ResetAt.Format(time.RFC3339),
	})
}
