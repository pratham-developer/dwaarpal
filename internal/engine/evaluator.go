package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/metrics"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
	go_redis "github.com/redis/go-redis/v9"
)

// Request defines the standard parameters required for a rate-limit check.
type Request struct {
	Key       string `json:"key"`
	Algorithm string `json:"algorithm"`
	Limit     int    `json:"limit"`
	Window    int    `json:"window"`
}

// Decision represents the final outcome determined by the engine.
type Decision struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration
	ResetAt    time.Time
	FailedKey  string
}

// Evaluator is the core engine responsible for pipelining requests, handling L1 cache,
// executing self-healing Lua scripts, and calculating the final outcome.
type Evaluator struct {
	RedisClient  *redis.Client
	Limiters     map[string]limiter.RateLimiter
	Timeout      time.Duration
	L1Cache      *lru.Cache[string, time.Time]
	MaxBatchSize int
	FailOpen     bool
}

// NewEvaluator creates a new instance of the core Evaluator engine.
func NewEvaluator(rc *redis.Client, limiters map[string]limiter.RateLimiter, timeout time.Duration, l1Cache *lru.Cache[string, time.Time], maxBatchSize int, failOpen bool) *Evaluator {
	return &Evaluator{
		RedisClient:  rc,
		Limiters:     limiters,
		Timeout:      timeout,
		L1Cache:      l1Cache,
		MaxBatchSize: maxBatchSize,
		FailOpen:     failOpen,
	}
}

// QueuedCheck holds internal tracking data for a queued pipeline command.
type QueuedCheck struct {
	Cmd        *go_redis.Cmd
	Descriptor Request
	Limiter    limiter.RateLimiter
}

// EvaluateBatch orchestrates the entire multi-key rate limit pipeline.
// It applies L1 short-circuiting, Redis cluster pipelining, NOSCRIPT self-healing,
// and best-effort response parsing.
func (e *Evaluator) EvaluateBatch(ctx context.Context, reqs []Request) (Decision, error) {
	if len(reqs) == 0 {
		return Decision{}, fmt.Errorf("empty requests array")
	}
	if len(reqs) > e.MaxBatchSize {
		return Decision{}, fmt.Errorf("batch size exceeds maximum limit of %d", e.MaxBatchSize)
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
		if reqs[i].Key == "" {
			return Decision{}, fmt.Errorf("missing 'key' in descriptor")
		}
	}

	// Phase A: L1 Cache (Penalty Box) Short-Circuit Check
	for _, req := range reqs {
		l1Key := fmt.Sprintf("%s:%s:%d:%d", req.Algorithm, req.Key, req.Limit, req.Window)
		if resetAt, ok := e.L1Cache.Get(l1Key); ok {
			if now.Before(resetAt) {
				// Cache hit! Entire batch fails instantly.
				slog.Info("L1 Cache Hit: Request batch blocked locally", "failed_key", l1Key)
				return Decision{
					Allowed:    false,
					FailedKey:  l1Key,
					Remaining:  0,
					RetryAfter: resetAt.Sub(now),
					ResetAt:    resetAt,
				}, nil
			}
			// Proactive Lazy Eviction: Remove expired keys to free LRU capacity
			e.L1Cache.Remove(l1Key)
		}
	}

	start := time.Now()
	evalCtx, cancel := context.WithTimeout(ctx, e.Timeout)
	defer cancel()

	// Phase B & C: Pipeline Queueing & Cluster-Aware Execution
	var queuedChecks []QueuedCheck
	var pipe go_redis.Pipeliner
	var pipelineErr error

	maxRetries := 1
	for attempt := 0; attempt <= maxRetries; attempt++ {
		pipe = e.RedisClient.Rdb.Pipeline()
		queuedChecks = nil // Reset for retry

		for i := range reqs {
			req := &reqs[i]
			limiterToUse, exists := e.Limiters[req.Algorithm]
			if !exists {
				return Decision{}, fmt.Errorf("unsupported algorithm: %s", req.Algorithm)
			}

			windowDur := time.Duration(req.Window) * time.Second
			cmd := limiterToUse.Queue(evalCtx, pipe, req.Key, req.Limit, windowDur, 1)

			queuedChecks = append(queuedChecks, QueuedCheck{
				Cmd:        cmd,
				Descriptor: *req,
				Limiter:    limiterToUse,
			})
		}

		_, pipelineErr = pipe.Exec(evalCtx)
		if pipelineErr != nil && pipelineErr != go_redis.Nil {
			// Self-Healing Mechanism for NOSCRIPT
			if go_redis.HasErrorPrefix(pipelineErr, "NOSCRIPT") && attempt < maxRetries {
				slog.Warn("NOSCRIPT detected in pipeline! Triggering Self-Healing Pre-Warm...")
				metrics.RedisErrorsTotal.WithLabelValues("MULTI_NOSCRIPT").Inc()

				if healErr := e.RedisClient.PreWarmAndSelfHeal(evalCtx); healErr != nil {
					slog.Error("Self-Healing Pre-Warm failed", "error", healErr)
				}
				continue // Retry loop will rebuild and re-execute pipeline
			}
			// DO NOT SHORT-CIRCUIT! Proceed to Best-Effort Parsing Phase
		}

		break
	}
	metrics.DecisionLatency.WithLabelValues("MULTI_PIPELINE").Observe(time.Since(start).Seconds())

	// Phase D: Parsing & Best-Effort Evaluation
	var failedResult *limiter.Result
	var failedL1Key string
	minRemaining := -1

	hadInfraError := pipelineErr != nil && pipelineErr != go_redis.Nil

	type FailedRecord struct {
		Key     string
		ResetAt time.Time
	}
	var allFailures []FailedRecord

	for _, q := range queuedChecks {
		if q.Cmd.Err() != nil && q.Cmd.Err() != go_redis.Nil {
			hadInfraError = true
			continue
		}

		res, err := q.Limiter.Parse(q.Cmd)
		if err != nil {
			metrics.RedisErrorsTotal.WithLabelValues("MULTI_PARSE_ERROR").Inc()
			slog.Error("Failed to parse pipeline result", "error", err, "key", q.Descriptor.Key)
			hadInfraError = true
			continue
		}

		if !res.Allowed {
			l1Key := fmt.Sprintf("%s:%s:%d:%d", q.Descriptor.Algorithm, q.Descriptor.Key, q.Descriptor.Limit, q.Descriptor.Window)
			res.ResetAt = time.Now().UTC().Add(res.RetryAfter)

			allFailures = append(allFailures, FailedRecord{
				Key:     l1Key,
				ResetAt: res.ResetAt,
			})

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

	// 1. A Definitive Block Trumps Everything!
	if failedResult != nil {
		metrics.RequestsTotal.WithLabelValues("MULTI", "false").Inc()
		for _, failure := range allFailures {
			e.L1Cache.Add(failure.Key, failure.ResetAt)
		}
		slog.Info("Multi-Key rate limit decision", "allowed", false, "failed_key", failedL1Key, "total_failures_cached", len(allFailures))
		return Decision{
			Allowed:    false,
			FailedKey:  failedL1Key,
			Remaining:  0,
			RetryAfter: failedResult.RetryAfter,
			ResetAt:    failedResult.ResetAt,
		}, nil
	}

	// 2. Infra error handling
	if hadInfraError {
		if e.FailOpen {
			slog.Warn("Pipeline execution failed (Fail Open - Allowing Traffic)", "error", pipelineErr)
			metrics.FailOpenTotal.Inc()
			return Decision{
				Allowed:   true,
				Remaining: 9999,
				ResetAt:   time.Now().UTC(),
			}, nil
		}
		metrics.RedisErrorsTotal.WithLabelValues("MULTI_FAIL_CLOSED").Inc()
		slog.Error("Pipeline execution failed (Fail Closed)", "error", pipelineErr)
		return Decision{}, fmt.Errorf("infrastructure error: %w", pipelineErr)
	}

	// 3. Success
	metrics.RequestsTotal.WithLabelValues("MULTI", "true").Inc()
	slog.Info("Multi-Key rate limit decision", "allowed", true, "min_remaining", minRemaining)

	return Decision{
		Allowed:   true,
		Remaining: minRemaining,
		ResetAt:   time.Now().UTC(),
	}, nil
}
