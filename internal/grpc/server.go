package grpc

import (
	"context"
	"log/slog"
	"strconv"
	"time"

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
}

// NewServer creates a new gRPC RateLimiter server.
func NewServer(limiters map[string]limiter.RateLimiter) *Server {
	return &Server{
		Limiters: limiters,
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
	start := time.Now()

	// Apply strict timeout for Fail Closed behavior (just like HTTP)
	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
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
			ResetAt:    time.Now().Format(time.RFC3339),
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

	return &proto.CheckRateLimitResponse{
		Allowed:    res.Allowed,
		Remaining:  int32(res.Remaining),
		RetryAfter: int64(res.RetryAfter.Milliseconds()),
		ResetAt:    res.ResetAt.Format(time.RFC3339),
	}, nil
}
