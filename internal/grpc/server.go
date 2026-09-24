package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prathamkhanduja/dwaarpal/api/proto"
	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/metrics"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
	go_redis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the RateLimiterService gRPC interface.
type Server struct {
	proto.UnimplementedRateLimiterServiceServer
	RedisClient  *redis.Client
	Limiters     map[string]limiter.RateLimiter
	Timeout      time.Duration
	L1Cache      *lru.Cache[string, time.Time]
	MaxBatchSize int
}

// NewServer creates a new gRPC RateLimiter server.
func NewServer(rc *redis.Client, limiters map[string]limiter.RateLimiter, timeout time.Duration, cache *lru.Cache[string, time.Time], maxBatchSize int) *Server {
	return &Server{
		RedisClient:  rc,
		Limiters:     limiters,
		Timeout:      timeout,
		L1Cache:      cache,
		MaxBatchSize: maxBatchSize,
	}
}

// QueuedCheck tracks the execution of a pipeline command for gRPC.
type QueuedCheck struct {
	Cmd        *go_redis.Cmd
	Descriptor *proto.RateLimitDescriptor
	Limiter    limiter.RateLimiter
}

// CheckRateLimit handles incoming gRPC rate limit requests using True Pipelining.
func (s *Server) CheckRateLimit(ctx context.Context, req *proto.CheckRateLimitRequest) (*proto.CheckRateLimitResponse, error) {
	if len(req.Descriptors) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing descriptors in request")
	}
	if len(req.Descriptors) > s.MaxBatchSize {
		return nil, status.Errorf(codes.InvalidArgument, "batch size exceeds maximum limit of %d", s.MaxBatchSize)
	}

	now := time.Now().UTC()

	// Apply defaults before evaluating
	for i := range req.Descriptors {
		desc := req.Descriptors[i]
		if desc.Limit <= 0 {
			desc.Limit = 100
		}
		if desc.Window <= 0 {
			desc.Window = 60
		}
		if desc.Algorithm == "" {
			desc.Algorithm = "TOKEN_BUCKET"
		}
	}

	// Phase A: L1 Cache (Penalty Box) Short-Circuit Check
	for _, desc := range req.Descriptors {
		l1Key := fmt.Sprintf("%s:%s:%d:%d", desc.Algorithm, desc.Key, desc.Limit, desc.Window)
		if resetAt, ok := s.L1Cache.Get(l1Key); ok {
			if now.Before(resetAt) {
				retryAfter := resetAt.Sub(now)
				slog.Info("gRPC L1 Cache Hit: Request batch blocked locally", "failed_key", l1Key)
				return &proto.CheckRateLimitResponse{
					Allowed:    false,
					FailedKey:  l1Key,
					Remaining:  0,
					RetryAfter: int64(retryAfter.Milliseconds()),
					ResetAt:    resetAt.Format(time.RFC3339),
				}, nil
			}
		}
	}

	start := time.Now()
	timeoutCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	// Phase B & C: Pipeline Queueing & Cluster-Aware Execution
	var queuedChecks []QueuedCheck
	var pipe go_redis.Pipeliner
	
	maxRetries := 1
	for attempt := 0; attempt <= maxRetries; attempt++ {
		pipe = s.RedisClient.Rdb.Pipeline()
		queuedChecks = nil // Reset for retry

		for i := range req.Descriptors {
			desc := req.Descriptors[i]
			
			limit := int(desc.Limit)
			if limit <= 0 { limit = 100 }
			
			window := int(desc.Window)
			if window <= 0 { window = 60 }
			
			algorithm := desc.Algorithm
			if algorithm == "" { algorithm = "TOKEN_BUCKET" }

			if desc.Key == "" {
				return nil, status.Error(codes.InvalidArgument, "missing 'key' in descriptor")
			}

			limiterToUse, exists := s.Limiters[algorithm]
			if !exists {
				return nil, status.Errorf(codes.InvalidArgument, "unsupported algorithm: %s", algorithm)
			}

			windowDur := time.Duration(window) * time.Second
			cmd := limiterToUse.Queue(timeoutCtx, pipe, desc.Key, limit, windowDur, 1)
			
			queuedChecks = append(queuedChecks, QueuedCheck{
				Cmd:        cmd,
				Descriptor: desc,
				Limiter:    limiterToUse,
			})
		}

		_, err := pipe.Exec(timeoutCtx)
		if err != nil && err != go_redis.Nil {
			if err.Error() == "NOSCRIPT No matching script. Please use EVAL." && attempt < maxRetries {
				slog.Warn("gRPC NOSCRIPT detected in pipeline! Triggering Self-Healing Pre-Warm...")
				metrics.RedisErrorsTotal.WithLabelValues("MULTI_NOSCRIPT").Inc()
				
				if healErr := s.RedisClient.PreWarmAndSelfHeal(timeoutCtx); healErr != nil {
					slog.Error("Self-Healing Pre-Warm failed", "error", healErr)
				}
				continue // Retry loop will rebuild and re-execute pipeline
			}
			
			metrics.RedisErrorsTotal.WithLabelValues("MULTI_FAIL_CLOSED").Inc()
			slog.Error("gRPC Pipeline execution failed (Fail Closed)", "error", err)
			return &proto.CheckRateLimitResponse{Allowed: false, Remaining: 0}, nil
		}
		
		break // Success, break the retry loop
	}
	metrics.DecisionLatency.WithLabelValues("MULTI_PIPELINE").Observe(time.Since(start).Seconds())

	// Phase D: Parsing & The "All-or-Nothing" Blackbox
	var failedResult *limiter.Result
	var failedL1Key string
	minRemaining := int32(-1)
	
	type FailedRecord struct {
		Key     string
		ResetAt time.Time
	}
	var allFailures []FailedRecord

	for _, q := range queuedChecks {
		res, err := q.Limiter.Parse(q.Cmd)
		if err != nil {
			metrics.RedisErrorsTotal.WithLabelValues("MULTI_PARSE_ERROR").Inc()
			slog.Error("gRPC Failed to parse pipeline result", "error", err, "key", q.Descriptor.Key)
			return &proto.CheckRateLimitResponse{Allowed: false, Remaining: 0}, nil
		}

		limit := int(q.Descriptor.Limit)
		if limit <= 0 { limit = 100 }
		window := int(q.Descriptor.Window)
		if window <= 0 { window = 60 }
		algorithm := q.Descriptor.Algorithm
		if algorithm == "" { algorithm = "TOKEN_BUCKET" }

		if !res.Allowed {
			l1Key := fmt.Sprintf("%s:%s:%d:%d", algorithm, q.Descriptor.Key, limit, window)
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
			if minRemaining == -1 || int32(res.Remaining) < minRemaining {
				minRemaining = int32(res.Remaining)
			}
		}
	}

	// 3. Aggregate Results and Populate Cache
	if failedResult != nil {
		metrics.RequestsTotal.WithLabelValues("MULTI", "false").Inc()
		
		for _, failure := range allFailures {
			s.L1Cache.Add(failure.Key, failure.ResetAt)
		}

		slog.Info("gRPC Multi-Key rate limit decision", "allowed", false, "failed_key", failedL1Key, "total_failures_cached", len(allFailures))

		return &proto.CheckRateLimitResponse{
			Allowed:    false,
			FailedKey:  failedL1Key,
			Remaining:  0,
			RetryAfter: int64(failedResult.RetryAfter.Milliseconds()),
			ResetAt:    failedResult.ResetAt.Format(time.RFC3339),
		}, nil
	}

	metrics.RequestsTotal.WithLabelValues("MULTI", "true").Inc()
	slog.Info("gRPC Multi-Key rate limit decision", "allowed", true, "min_remaining", minRemaining)

	return &proto.CheckRateLimitResponse{
		Allowed:    true,
		Remaining:  minRemaining,
		RetryAfter: 0,
		ResetAt:    time.Now().UTC().Format(time.RFC3339),
	}, nil
}
