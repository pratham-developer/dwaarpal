package redis

import (
	"context"
	"fmt"
	"strings"

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

	// Ping the Redis server to verify connection.
	if err := rdb.Ping(context.Background()).Err(); err != nil {
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
