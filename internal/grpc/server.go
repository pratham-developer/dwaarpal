package grpc

import (
	"context"
	"time"

	"github.com/prathamkhanduja/dwaarpal/api/proto"
	"github.com/prathamkhanduja/dwaarpal/internal/engine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the RateLimiterService gRPC interface.
type Server struct {
	proto.UnimplementedRateLimiterServiceServer
	Engine *engine.Evaluator
}

// NewServer creates a new gRPC RateLimiter server.
func NewServer(e *engine.Evaluator) *Server {
	return &Server{
		Engine: e,
	}
}

// CheckRateLimit handles incoming gRPC rate limit requests via the central engine.
func (s *Server) CheckRateLimit(ctx context.Context, req *proto.CheckRateLimitRequest) (*proto.CheckRateLimitResponse, error) {
	if len(req.Descriptors) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing descriptors in request")
	}

	engineReqs := make([]engine.Request, len(req.Descriptors))
	for i, desc := range req.Descriptors {
		engineReqs[i] = engine.Request{
			Key:       desc.Key,
			Algorithm: desc.Algorithm,
			Limit:     int(desc.Limit),
			Window:    int(desc.Window),
		}
	}

	decision, err := s.Engine.EvaluateBatch(ctx, engineReqs)

	if err != nil {
		if err.Error() == "empty requests array" || err.Error()[:10] == "batch size" || err.Error()[:7] == "missing" || err.Error()[:11] == "unsupported" {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		// Infrastructure error where fail-open is disabled
		return &proto.CheckRateLimitResponse{
			Allowed:   false,
			Remaining: 0,
		}, nil
	}

	return &proto.CheckRateLimitResponse{
		Allowed:    decision.Allowed,
		FailedKey:  decision.FailedKey,
		Remaining:  int32(decision.Remaining),
		RetryAfter: int64(decision.RetryAfter.Milliseconds()),
		ResetAt:    decision.ResetAt.Format(time.RFC3339),
	}, nil
}
