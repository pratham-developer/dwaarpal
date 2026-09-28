package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/metrics"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
	go_redis "github.com/redis/go-redis/v9"
)

// Handler holds dependencies for the HTTP handlers.
type Handler struct {
	RedisClient  *redis.Client
	Limiters     map[string]limiter.RateLimiter
	Timeout      time.Duration
	L1Cache      *lru.Cache[string, time.Time]
	MaxBatchSize int
	FailOpen     bool
}

// NewHandler creates a new Handler.
func NewHandler(rc *redis.Client, limiters map[string]limiter.RateLimiter, timeout time.Duration, l1CacheSize int, maxBatchSize int, failOpen bool) *Handler {
	cache, _ := lru.New[string, time.Time](l1CacheSize)
	return &Handler{
		RedisClient:  rc,
		Limiters:     limiters,
		Timeout:      timeout,
		L1Cache:      cache,
		MaxBatchSize: maxBatchSize,
		FailOpen:     failOpen,
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

// RateLimitDescriptor represents a single rule to evaluate.
type RateLimitDescriptor struct {
	Key       string `json:"key"`
	Algorithm string `json:"algorithm"`
	Limit     int    `json:"limit"`
	Window    int    `json:"window"` // window in seconds
}

// CheckMultiResponse represents the aggregated result.
type CheckMultiResponse struct {
	Allowed    bool   `json:"allowed"`
	FailedKey  string `json:"failed_key,omitempty"`
	Remaining  int    `json:"remaining"`
	RetryAfter int64  `json:"retry_after"` // milliseconds
	ResetAt    string `json:"reset_at"`
}

// QueuedCheck tracks the execution of a pipeline command.
type QueuedCheck struct {
	Cmd        *go_redis.Cmd
	Descriptor RateLimitDescriptor
	Limiter    limiter.RateLimiter
}

// CheckRateLimit handles multi-key rate-limiting decisions using True Pipelining.
func (h *Handler) CheckRateLimit(w http.ResponseWriter, r *http.Request) {
	var reqs []RateLimitDescriptor
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		http.Error(w, "Invalid JSON payload, expected an array of descriptors", http.StatusBadRequest)
		return
	}
	if len(reqs) == 0 {
		http.Error(w, "Empty requests array", http.StatusBadRequest)
		return
	}
	if len(reqs) > h.MaxBatchSize {
		http.Error(w, fmt.Sprintf("Batch size exceeds maximum limit of %d", h.MaxBatchSize), http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()

	// Apply defaults before evaluating
	for i := range reqs {
		if reqs[i].Limit <= 0 {
			reqs[i].Limit = 100
		}
		if reqs[i].Window <= 0 {
			reqs[i].Window = 60
		}
		if reqs[i].Algorithm == "" {
			reqs[i].Algorithm = "TOKEN_BUCKET"
		}
	}

	// Phase A: L1 Cache (Penalty Box) Short-Circuit Check
	for _, req := range reqs {
		l1Key := fmt.Sprintf("%s:%s:%d:%d", req.Algorithm, req.Key, req.Limit, req.Window)
		if resetAt, ok := h.L1Cache.Get(l1Key); ok {
			if now.Before(resetAt) {
				// Cache hit! Entire batch fails instantly.
				retryAfter := resetAt.Sub(now)
				slog.Info("L1 Cache Hit: Request batch blocked locally", "failed_key", l1Key)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				json.NewEncoder(w).Encode(CheckMultiResponse{
					Allowed:    false,
					FailedKey:  l1Key,
					Remaining:  0,
					RetryAfter: retryAfter.Milliseconds(),
					ResetAt:    resetAt.Format(time.RFC3339),
				})
				return
			}
		}
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), h.Timeout)
	defer cancel()

	// Phase B & C: Pipeline Queueing & Cluster-Aware Execution
	var queuedChecks []QueuedCheck
	var pipe go_redis.Pipeliner
	var pipelineErr error
	
	maxRetries := 1
	for attempt := 0; attempt <= maxRetries; attempt++ {
		pipe = h.RedisClient.Rdb.Pipeline()
		queuedChecks = nil // Reset for retry
		
		for i := range reqs {
			req := &reqs[i]
			// Set sensible defaults
			if req.Limit <= 0 {
				req.Limit = 100
			}
			if req.Window <= 0 {
				req.Window = 60
			}
			if req.Algorithm == "" {
				req.Algorithm = "TOKEN_BUCKET"
			}
			if req.Key == "" {
				http.Error(w, "Missing 'key' in descriptor", http.StatusBadRequest)
				return
			}
	
			limiterToUse, exists := h.Limiters[req.Algorithm]
			if !exists {
				http.Error(w, fmt.Sprintf("Unsupported algorithm: %s", req.Algorithm), http.StatusBadRequest)
				return
			}
	
			windowDur := time.Duration(req.Window) * time.Second
			cmd := limiterToUse.Queue(ctx, pipe, req.Key, req.Limit, windowDur, 1)
			
			queuedChecks = append(queuedChecks, QueuedCheck{
				Cmd:        cmd,
				Descriptor: *req,
				Limiter:    limiterToUse,
			})
		}
	
		_, pipelineErr = pipe.Exec(ctx)
		if pipelineErr != nil && pipelineErr != go_redis.Nil {
			// Self-Healing Mechanism for NOSCRIPT
			if go_redis.HasErrorPrefix(pipelineErr, "NOSCRIPT") && attempt < maxRetries {
				slog.Warn("NOSCRIPT detected in pipeline! Triggering Self-Healing Pre-Warm...")
				metrics.RedisErrorsTotal.WithLabelValues("MULTI_NOSCRIPT").Inc()
				
				// Background repair (synchronous for this request so we don't drop it)
				if healErr := h.RedisClient.PreWarmAndSelfHeal(ctx); healErr != nil {
					slog.Error("Self-Healing Pre-Warm failed", "error", healErr)
				}
				continue // Retry loop will rebuild and re-execute pipeline
			}
			
			// DO NOT SHORT-CIRCUIT! Proceed to Best-Effort Parsing Phase to catch surviving blocks.
		}
		
		break // Success or non-retryable error, break the retry loop
	}
	metrics.DecisionLatency.WithLabelValues("MULTI_PIPELINE").Observe(time.Since(start).Seconds())

	// Phase D: Parsing & Best-Effort Evaluation
	var failedResult *limiter.Result
	var failedL1Key string
	minRemaining := -1
	
	// Track if we had any infrastructure/parse errors during evaluation
	hadInfraError := pipelineErr != nil && pipelineErr != go_redis.Nil
	
	// Collect all failures to populate L1 cache comprehensively
	type FailedRecord struct {
		Key     string
		ResetAt time.Time
	}
	var allFailures []FailedRecord

	for _, q := range queuedChecks {
		// If the command itself failed during pipeline execution, skip parsing.
		if q.Cmd.Err() != nil && q.Cmd.Err() != go_redis.Nil {
			hadInfraError = true
			continue
		}

		res, err := q.Limiter.Parse(q.Cmd)
		if err != nil {
			metrics.RedisErrorsTotal.WithLabelValues("MULTI_PARSE_ERROR").Inc()
			slog.Error("Failed to parse pipeline result", "error", err, "key", q.Descriptor.Key)
			hadInfraError = true
			continue // Skip this one, but keep evaluating others for potential BLOCKS!
		}

		if !res.Allowed {
			l1Key := fmt.Sprintf("%s:%s:%d:%d", q.Descriptor.Algorithm, q.Descriptor.Key, q.Descriptor.Limit, q.Descriptor.Window)
			res.ResetAt = time.Now().UTC().Add(res.RetryAfter)
			
			// Add every single failed key to our batch array
			allFailures = append(allFailures, FailedRecord{
				Key:     l1Key,
				ResetAt: res.ResetAt,
			})

			// Track the most restrictive failure for the Gateway response
			if failedResult == nil || res.RetryAfter > failedResult.RetryAfter {
				resCopy := res
				failedResult = &resCopy
				failedL1Key = l1Key
			}
		} else {
			if minRemaining == -1 || res.Remaining < minRemaining {
				minRemaining = res.Remaining
			}
		}
	}

	// 3. Aggregate Results and Terminal Decision
	w.Header().Set("Content-Type", "application/json")

	// 1. A Definitive Block Trumps Everything!
	if failedResult != nil {
		metrics.RequestsTotal.WithLabelValues("MULTI", "false").Inc()

		// Populate L1 Cache with ALL failed keys
		for _, failure := range allFailures {
			h.L1Cache.Add(failure.Key, failure.ResetAt)
		}

		slog.Info("Multi-Key rate limit decision", "allowed", false, "failed_key", failedL1Key, "total_failures_cached", len(allFailures))

		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(CheckMultiResponse{
			Allowed:    false,
			FailedKey:  failedL1Key,
			Remaining:  0,
			RetryAfter: failedResult.RetryAfter.Milliseconds(),
			ResetAt:    failedResult.ResetAt.Format(time.RFC3339),
		})
		return
	}

	// 2. We didn't find any blocks, but an infrastructure error occurred (partial or full).
	if hadInfraError {
		if h.FailOpen {
			slog.Warn("Pipeline execution failed (Fail Open - Allowing Traffic)", "error", pipelineErr)
			metrics.FailOpenTotal.Inc()
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(CheckMultiResponse{
				Allowed:   true,
				Remaining: 9999,
				ResetAt:   time.Now().UTC().Format(time.RFC3339),
			})
			return
		}
		
		metrics.RedisErrorsTotal.WithLabelValues("MULTI_FAIL_CLOSED").Inc()
		slog.Error("Pipeline execution failed (Fail Closed)", "error", pipelineErr)
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(CheckMultiResponse{
			Allowed:   false,
			Remaining: 0,
		})
		return
	}

	// 3. Perfect Success!
	metrics.RequestsTotal.WithLabelValues("MULTI", "true").Inc()
	slog.Info("Multi-Key rate limit decision", "allowed", true, "min_remaining", minRemaining)

	json.NewEncoder(w).Encode(CheckMultiResponse{
		Allowed:   true,
		Remaining: minRemaining,
		ResetAt:   time.Now().UTC().Format(time.RFC3339),
	})
}
