package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prathamkhanduja/dwaarpal/api/proto"
	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the RateLimiterService gRPC interface.
type Server struct {
	proto.UnimplementedRateLimiterServiceServer
	Limiters map[string]limiter.RateLimiter
	Timeout  time.Duration
	L1Cache  *lru.Cache[string, time.Time]
}

// NewServer creates a new gRPC RateLimiter server.
func NewServer(limiters map[string]limiter.RateLimiter, timeout time.Duration, cache *lru.Cache[string, time.Time]) *Server {
	return &Server{
		Limiters: limiters,
		Timeout:  timeout,
		L1Cache:  cache,
	}
}

// CheckRateLimit handles incoming gRPC rate limit requests.
func (s *Server) CheckRateLimit(ctx context.Context, req *proto.CheckRateLimitRequest) (*proto.CheckRateLimitResponse, error) {
	// Set sensible defaults if not provided
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}

	window := int(req.Window)
	if window <= 0 {
		window = 60
	}

	key := req.Key
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "missing 'key' in request")
	}

	algorithm := req.Algorithm
	if algorithm == "" {
		algorithm = "TOKEN_BUCKET"
	}

	limiterToUse, exists := s.Limiters[algorithm]
	if !exists {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported algorithm: %s", algorithm)
	}

	windowDuration := time.Duration(window) * time.Second

	// L1 Cache (Penalty Box) Check
	l1Key := fmt.Sprintf("%s:%s:%d:%d", algorithm, key, limit, window)
	now := time.Now().UTC()

	if resetAt, ok := s.L1Cache.Get(l1Key); ok {
		if now.Before(resetAt) {
			retryAfter := resetAt.Sub(now)
			slog.Info("gRPC L1 Cache Hit: Request blocked locally", "key", l1Key, "retryAfter", retryAfter)
			return &proto.CheckRateLimitResponse{
				Allowed:    false,
				Remaining:  0,
				RetryAfter: int64(retryAfter.Milliseconds()),
				ResetAt:    resetAt.Format(time.RFC3339),
			}, nil
		}
	}

	start := time.Now()

	// Apply strict timeout for Fail Closed behavior (just like HTTP)
	timeoutCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	// Call the underlying rate limiter
	res, err := limiterToUse.Allow(timeoutCtx, key, limit, windowDuration, 1)

	metrics.DecisionLatency.WithLabelValues(algorithm).Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.RedisErrorsTotal.WithLabelValues(algorithm).Inc()
		slog.Error("gRPC rate limit decision failed, failing closed", "error", err, "key", key, "algorithm", algorithm)

		// Fail Closed: Return an explicit response rejecting the request
		return &proto.CheckRateLimitResponse{
			Allowed:    false,
			Remaining:  0,
			RetryAfter: 0,
			ResetAt:    time.Now().UTC().Format(time.RFC3339),
		}, nil
	}

	metrics.RequestsTotal.WithLabelValues(algorithm, strconv.FormatBool(res.Allowed)).Inc()

	slog.Info("gRPC rate limit decision",
		"key", key,
		"algorithm", algorithm,
		"allowed", res.Allowed,
		"remaining", res.Remaining,
		"retryAfter", res.RetryAfter,
	)

	// L1 Cache Population (Negative Caching)
	if !res.Allowed {
		res.ResetAt = time.Now().UTC().Add(res.RetryAfter)
		s.L1Cache.Add(l1Key, res.ResetAt)
	} else {
		res.ResetAt = time.Now().UTC().Add(res.RetryAfter)
	}

	return &proto.CheckRateLimitResponse{
		Allowed:    res.Allowed,
		Remaining:  int32(res.Remaining),
		RetryAfter: int64(res.RetryAfter.Milliseconds()),
		ResetAt:    res.ResetAt.Format(time.RFC3339),
	}, nil
}
