package integration

import (
	"context"
	"sync"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/limiter"
	"github.com/prathamkhanduja/dwaarpal/internal/redis"
)

var preWarmOnce sync.Once

func allowHelper(rc *redis.Client, l limiter.RateLimiter, ctx context.Context, key string, limit int, window time.Duration, cost int) (limiter.Result, error) {
	preWarmOnce.Do(func() {
		rc.PreWarmAndSelfHeal(context.Background())
	})

	pipe := rc.Rdb.Pipeline()
	cmd := l.Queue(ctx, pipe, key, limit, window, cost)
	
	_, err := pipe.Exec(ctx)
	if err != nil {
		return limiter.Result{}, err
	}
	
	return l.Parse(cmd)
}
