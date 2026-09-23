package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Client wraps the Redis client.
type Client struct {
	Rdb *redis.Client
}

// NewClient initializes and returns a new Redis client.
func NewClient(addr string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})

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
