package redis

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/prathamkhanduja/dwaarpal/internal/redis/scripts"
	"github.com/redis/go-redis/v9"
)

// Client wraps the Redis client.
type Client struct {
	Rdb redis.UniversalClient
}

// NewClient initializes and returns a new Redis client.
func NewClient(addr string) (*Client, error) {
	var rdb redis.UniversalClient

	if strings.HasPrefix(addr, "redis://") || strings.HasPrefix(addr, "rediss://") {
		opts, err := redis.ParseURL(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid redis URL: %w", err)
		}
		rdb = redis.NewClient(opts)
	} else if strings.Contains(addr, ",") {
		addrs := strings.Split(addr, ",")
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addrs,
		})
	} else {
		rdb = redis.NewClient(&redis.Options{
			Addr: addr,
		})
	}

	// Ping the Redis server to verify connection with retries for slow cluster boots.
	var err error
	maxRetries := 15
	for i := 0; i < maxRetries; i++ {
		err = rdb.Ping(context.Background()).Err()
		if err == nil {
			break
		}
		log.Printf("Waiting for Redis at %s to become available (attempt %d/%d)... err: %v", addr, i+1, maxRetries, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to redis at %s: %w", addr, err)
	}

	return &Client{
		Rdb: rdb,
	}, nil
}

// Close gracefully closes the Redis connection.
func (c *Client) Close() error {
	return c.Rdb.Close()
}

// PreWarmAndSelfHeal loads all scripts to all master nodes in the cluster.
// This prevents NOSCRIPT errors when using pipelines.
func (c *Client) PreWarmAndSelfHeal(ctx context.Context) error {
	load := func(r redis.Cmdable) error {
		var err error
		if scripts.FixedWindowSHA, err = r.ScriptLoad(ctx, scripts.FixedWindow).Result(); err != nil {
			return err
		}
		if scripts.SlidingWindowLogSHA, err = r.ScriptLoad(ctx, scripts.SlidingWindowLog).Result(); err != nil {
			return err
		}
		if scripts.TokenBucketSHA, err = r.ScriptLoad(ctx, scripts.TokenBucket).Result(); err != nil {
			return err
		}
		if scripts.SlidingWindowCounterSHA, err = r.ScriptLoad(ctx, scripts.SlidingWindowCounter).Result(); err != nil {
			return err
		}
		if scripts.LeakyBucketSHA, err = r.ScriptLoad(ctx, scripts.LeakyBucket).Result(); err != nil {
			return err
		}
		return nil
	}

	switch rdb := c.Rdb.(type) {
	case *redis.ClusterClient:
		return rdb.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			return load(client)
		})
	default:
		return load(c.Rdb)
	}
}
