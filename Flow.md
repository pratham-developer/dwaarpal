# Dwaarpal - Distributed Rate Limiter in Go

A production-oriented **distributed rate limiter** built with **Go and Redis**, designed to explore distributed systems concepts such as atomicity, concurrency, shared state, horizontal scaling, failure handling, and observability.

The system is designed to run across multiple stateless Go instances while maintaining globally consistent rate-limit state in Redis.

---

## 1. Goals

### Primary Goals

* Build a standalone rate-limiting service in Go.
* Support multiple rate-limiting algorithms.
* Maintain shared state across multiple Go instances.
* Guarantee atomic rate-limit decisions under high concurrency.
* Use Redis Lua scripts for atomic state transitions.
* Support horizontal scaling.
* Measure throughput and latency under concurrent load.
* Provide metrics and observability.
* Handle Redis failures explicitly.

### Algorithms

The project will implement:

1. Fixed Window
2. Sliding Window
3. Token Bucket

The **Token Bucket** implementation will be the primary production-oriented implementation.

---

# 2. High-Level Architecture

```text
                           ┌─────────────────┐
                           │     Client      │
                           └────────┬────────┘
                                    │
                                    ▼
                           ┌─────────────────┐
                           │ Load Balancer   │
                           └────────┬────────┘
                                    │
                  ┌─────────────────┼─────────────────┐
                  │                 │                 │
                  ▼                 ▼                 ▼
            ┌──────────┐      ┌──────────┐      ┌──────────┐
            │ Go Node 1│      │ Go Node 2│      │ Go Node 3│
            └─────┬────┘      └─────┬────┘      └─────┬────┘
                  │                 │                 │
                  └─────────────────┼─────────────────┘
                                    │
                                    ▼
                           ┌─────────────────┐
                           │ Redis Cluster   │
                           │                 │
                           │ Rate State      │
                           │ Lua Scripts     │
                           └────────┬────────┘
                                    │
                     ┌──────────────┴──────────────┐
                     ▼                             ▼
              ┌─────────────┐               ┌─────────────┐
              │ Prometheus  │               │ Structured  │
              │             │               │ Logs        │
              └──────┬──────┘               └─────────────┘
                     │
                     ▼
                ┌─────────┐
                │ Grafana │
                └─────────┘
```

### Core principle

> **Go instances are stateless. Redis owns the distributed rate-limit state.**

Therefore:

```text
Request A → Go #1 ─┐
Request B → Go #2 ─┼──→ Redis
Request C → Go #3 ─┘
```

All instances make decisions against the same state.

No sticky sessions are required.

---

# 3. Why Redis?

A distributed rate limiter needs shared state.

A local implementation such as:

```go
map[string]*rate.Limiter
```

works only inside one process.

With multiple instances:

```text
             Load Balancer
              /    |    \
             /     |     \
         Go #1   Go #2   Go #3
           │       │       │
       local     local    local
       state     state    state
```

The system could allow significantly more requests than the configured limit.

Redis provides:

* Shared state
* Very low latency
* Atomic primitives
* TTL support
* Sorted sets
* Lua scripting
* Horizontal scalability through Redis Cluster

---

# 4. Functional Requirements

## Rate Limit Check

The service should expose:

```http
POST /v1/check
```

Request:

```json
{
  "key": "user:123",
  "limit": 100,
  "window": 60
}
```

Response when allowed:

```json
{
  "allowed": true,
  "remaining": 73,
  "limit": 100,
  "resetAt": 1727098800
}
```

Response when rejected:

```json
{
  "allowed": false,
  "remaining": 0,
  "limit": 100,
  "retryAfter": 12
}
```

---

# 5. Rate-Limiting Keys

The limiter should support arbitrary logical keys.

Examples:

```text
user:123
ip:10.20.30.40
api-key:abc123
tenant:company-a
endpoint:user-login
```

This allows different policies to be implemented later.

For example:

```text
user:123
    → 100 requests/minute

ip:10.20.30.40
    → 500 requests/minute

endpoint:login
    → 10 requests/minute
```

---

# 6. Fixed Window Algorithm

The simplest implementation.

Suppose:

```text
limit = 100
window = 60 seconds
```

Redis key:

```text
rl:user:123:window:<window-id>
```

Example:

```text
rl:user:123:window:28784978
```

The algorithm:

```text
Request
   │
   ▼
INCR Redis Key
   │
   ├── count <= limit → ALLOW
   │
   └── count > limit  → REJECT
```

The key receives a TTL equal to the window duration.

## Why Lua?

Avoid doing:

```text
INCR
EXPIRE
GET
```

as separate operations.

Multiple Go instances could interleave these operations.

Instead:

```text
Go
 │
 ▼
Lua Script
 │
 ├── INCR
 ├── EXPIRE
 ├── GET
 └── Return decision
```

Redis executes the Lua script atomically.

---

# 7. Sliding Window Algorithm

Fixed windows have a boundary problem.

Example:

```text
Window 1                  Window 2

59s  59s  59s | 00s 00s 00s
 └──── requests ────┘
```

A client could potentially send many requests near the boundary and effectively exceed the intended rate.

Sliding Window tracks actual request timestamps.

Use a Redis Sorted Set:

```text
Key:

rl:user:123
```

Example:

```text
score             member

1727098701        request-1
1727098705        request-2
1727098710        request-3
1727098715        request-4
```

For every request:

```text
1. Remove timestamps older than the window
2. Count remaining requests
3. If count < limit:
      add current request
      allow
4. Otherwise:
      reject
```

All operations happen inside one Lua script.

---

# 8. Token Bucket Algorithm

This is the primary implementation.

Suppose:

```text
capacity   = 100 tokens
refillRate = 10 tokens/sec
cost       = 1 token/request
```

Conceptually:

```text
                    refill
                       │
                       ▼
                ┌──────────────┐
                │ TOKEN BUCKET │
                │              │
                │ ● ● ● ● ●    │
                │ ● ● ●        │
                └──────┬───────┘
                       │
                    request
                       │
                 ┌─────┴─────┐
                 │           │
             token exists   empty
                 │           │
               ALLOW        REJECT
```

---

# 9. Token Bucket State

Redis stores state similar to:

```json
{
  "tokens": 73.4,
  "lastRefill": 1727098715000
}
```

For every request:

```text
elapsed = currentTime - lastRefill

newTokens = elapsed × refillRate

tokens = min(
    capacity,
    tokens + newTokens
)
```

Then:

```text
tokens >= requestCost
        │
        ├── YES → tokens -= requestCost → ALLOW
        │
        └── NO  → REJECT
```

The complete operation must be atomic.

---

# 10. Why Lua Is Important

Without Lua:

```text
Go #1 ── GET tokens ──┐
                      │
Go #2 ── GET tokens ──┤
                      ▼
                    Redis
```

Both instances could observe the same token count.

Example:

```text
tokens = 1

Go #1 → sees 1
Go #2 → sees 1

Go #1 → consumes token
Go #2 → consumes token
```

Two requests are allowed when only one should have been.

Lua solves this by making:

```text
read state
calculate refill
check tokens
consume token
update state
return result
```

one atomic Redis operation.

---

# 11. Token Bucket Redis Lua Flow

```text
Request
   │
   ▼
Redis Lua Script
   │
   ├── Read tokens
   │
   ├── Read lastRefill
   │
   ├── Calculate elapsed time
   │
   ├── Calculate refill
   │
   ├── Cap tokens at capacity
   │
   ├── Check requested cost
   │
   ├── If allowed
   │      └── subtract cost
   │
   ├── Update Redis state
   │
   └── Return
          ├── allowed
          ├── remaining
          └── retryAfter
```

---

# 12. Redis Key Design

For token bucket:

```text
rl:token:{user:123}
```

Example state:

```text
tokens = 72.5
last_refill = 1727098715000
```

A Redis Hash can be used:

```text
HSET rl:token:{user:123}
    tokens 72.5
    last_refill 1727098715000
```

The key should have a TTL so inactive rate-limit identities eventually disappear.

Example:

```text
EXPIRE rl:token:{user:123} 3600
```

This prevents unbounded memory growth from users that are no longer active.

---

# 13. Redis Cluster Considerations

Eventually the system should support:

```text
                 Redis Cluster
          ┌──────────┼──────────┐
          ▼          ▼          ▼
       Master 1   Master 2   Master 3
          │          │          │
       Replica    Replica    Replica
```

Use Redis hash tags when multiple related keys need to be colocated.

Example:

```text
rl:{user:123}:tokens
rl:{user:123}:metadata
```

The `{user:123}` portion determines the Redis Cluster hash slot.

This becomes important if a Lua script needs multiple keys.

---

# 14. API Design

## `POST /v1/check`

Request:

```json
{
  "key": "user:123",
  "algorithm": "TOKEN_BUCKET",
  "limit": 100,
  "window": 60
}
```

For token bucket, this can internally translate into:

```text
capacity = 100
refillRate = 100 / 60
```

Response:

```json
{
  "allowed": true,
  "remaining": 72,
  "limit": 100,
  "retryAfter": 0,
  "resetAt": 1727098800
}
```

---

# 15. Suggested Go Project Structure

```text
distributed-rate-limiter/
│
├── cmd/
│   └── server/
│       └── main.go
│
├── internal/
│   │
│   ├── limiter/
│   │   ├── limiter.go
│   │   ├── fixed_window.go
│   │   ├── sliding_window.go
│   │   └── token_bucket.go
│   │
│   ├── redis/
│   │   ├── client.go
│   │   └── scripts/
│   │       ├── fixed_window.lua
│   │       ├── sliding_window.lua
│   │       └── token_bucket.lua
│   │
│   ├── handler/
│   │   └── handler.go
│   │
│   ├── config/
│   │   └── config.go
│   │
│   └── metrics/
│       └── metrics.go
│
├── tests/
│   ├── integration/
│   └── load/
│
├── deployments/
│   └── docker-compose.yml
│
├── Dockerfile
├── go.mod
└── README.md
```

---

# 16. Go Interfaces

Keep the algorithm independent from the HTTP layer.

```go
type RateLimiter interface {
    Allow(
        ctx context.Context,
        key string,
        cost int,
    ) (Result, error)
}
```

Result:

```go
type Result struct {
    Allowed    bool
    Remaining  float64
    RetryAfter time.Duration
    ResetAt    time.Time
}
```

Then:

```text
RateLimiter
     │
     ├── FixedWindowLimiter
     │
     ├── SlidingWindowLimiter
     │
     └── TokenBucketLimiter
```

This makes algorithms independently testable.

---

# 17. Configuration

Example:

```yaml
server:
  port: 8080

redis:
  address: redis:6379
  password: ""
  database: 0

rateLimiter:
  algorithm: TOKEN_BUCKET
  capacity: 100
  refillRate: 10
  keyTtl: 3600
```

Eventually support per-policy configuration.

---

# 18. Failure Handling

Redis is a critical dependency.

The system needs an explicit failure policy.

## Fail Open

```text
Redis unavailable
       │
       ▼
ALLOW
```

Advantages:

* Higher availability
* Doesn't block legitimate traffic

Disadvantage:

* Rate limit can be bypassed during Redis failure

---

## Fail Closed

```text
Redis unavailable
       │
       ▼
REJECT
```

Advantages:

* Protects downstream systems

Disadvantages:

* Reduced availability
* Redis becomes a hard dependency

---

## Local Emergency Limiter

A hybrid approach:

```text
               Redis
                 │
          ┌──────┴──────┐
          │             │
       success        failure
          │             │
          ▼             ▼
     Redis limit    Local limiter
```

The local limiter should only act as an emergency protection mechanism.

It should not be treated as the authoritative distributed state.

---

# 19. Time Handling

Token Bucket depends on time.

Prefer calculating elapsed time from timestamps stored in Redis.

Conceptually:

```text
elapsed = now - lastRefill
```

The Lua script should receive the current timestamp consistently.

The implementation should also consider:

* Clock differences between Go instances
* Redis server time
* Timestamp precision
* Integer vs floating-point token calculations

A later optimization can use Redis server time:

```text
TIME
```

to reduce dependence on application-instance clocks.

---

# 20. Concurrency Guarantees

The key correctness property:

> For a given rate-limit key, concurrent requests must never cause the system to exceed the configured limit because of a race between instances.

Example:

```text
limit = 100

10,000 concurrent requests
             │
       ┌─────┴─────┐
       ▼           ▼
    Go #1         Go #2
       │           │
       └─────┬─────┘
             ▼
           Redis
             │
          Lua script
             │
             ▼
       atomic decision
```

Expected:

```text
allowed <= configured limit
```

for the chosen algorithm/window semantics.

---

# 21. Load Testing

Create a dedicated Go load-testing tool.

Example:

```bash
go run ./tests/load \
    --requests=10000 \
    --concurrency=500 \
    --key=user:123
```

Measure:

```text
Total requests
Successful requests
Rejected requests
Requests/sec
Average latency
p50 latency
p95 latency
p99 latency
Redis errors
```

Example output:

```text
Requests:       10000
Concurrency:    500
Allowed:        100
Rejected:       9900

Throughput:     18,421 req/sec

Latency:
p50:            2.1 ms
p95:            4.8 ms
p99:            7.2 ms
```

The numbers above are illustrative; actual benchmark results should come from your environment.

---

# 22. Multi-Instance Testing

Run multiple Go instances:

```text
Go #1 : 8081
Go #2 : 8082
Go #3 : 8083
```

Then:

```text
                 Load Generator
                       │
                       ▼
                 Load Balancer
                  /     |     \
                 /      |      \
              Go #1   Go #2   Go #3
                 \      |      /
                  \     |     /
                     Redis
```

Run thousands of concurrent requests.

Verify that the combined result across all Go instances respects the configured policy.

---

# 23. Observability

Expose:

```http
GET /metrics
```

Metrics:

```text
rate_limit_requests_total
rate_limit_allowed_total
rate_limit_rejected_total

rate_limit_redis_requests_total
rate_limit_redis_errors_total
rate_limit_redis_latency_seconds

rate_limit_decision_latency_seconds
```

Useful Grafana dashboards:

### Traffic

```text
Requests/sec
Allowed/sec
Rejected/sec
```

### Latency

```text
p50
p95
p99
```

### Redis

```text
Redis latency
Redis errors
Redis connection count
```

### Rate limiting

```text
Allowed / Rejected ratio
Top rate-limited keys
```

---

# 24. Logging

Use structured logs.

Example:

```json
{
  "level": "INFO",
  "event": "rate_limit_decision",
  "key": "user:123",
  "allowed": true,
  "remaining": 72,
  "algorithm": "TOKEN_BUCKET"
}
```

Do not log sensitive API keys or authentication credentials directly.

---

# 25. Docker Compose Development Environment

Development environment:

```text
docker-compose
│
├── rate-limiter-1
├── rate-limiter-2
├── rate-limiter-3
│
├── redis
│
├── prometheus
│
└── grafana
```

This allows the distributed architecture to be reproduced locally.

---

# 26. Testing Strategy

## Unit Tests

Test the algorithm independently.

Examples:

```text
bucket initially full
request consumes token
bucket refills
bucket never exceeds capacity
request rejected when empty
retryAfter is correct
```

---

## Lua Tests

Test Redis scripts against a real Redis instance.

Verify:

```text
atomicity
TTL
concurrent requests
boundary conditions
```

---

## Integration Tests

```text
Go API
   ↓
Redis
```

Test the entire request flow.

---

## Concurrency Tests

Important scenarios:

```text
100 goroutines
1,000 goroutines
10,000 requests
multiple Go instances
same rate-limit key
```

The primary correctness invariant:

```text
allowed requests <= configured allowance
```

according to the algorithm's semantics.

---

# 27. Important Edge Cases

Handle:

* Empty rate-limit key
* Invalid limit
* Invalid refill rate
* Zero capacity
* Negative request cost
* Extremely large capacity
* Redis timeout
* Redis connection failure
* Redis key expiration
* Concurrent requests
* Duplicate requests
* Inactive keys
* Hot keys
* Large number of unique keys

---

# 28. Hot Key Problem

Consider:

```text
user:123
```

receiving:

```text
1,000,000 requests/sec
```

All requests target the same Redis key.

That creates a hot key.

Architecture:

```text
Go #1 ─┐
Go #2 ─┤
Go #3 ─┤
Go #4 ─┤
       │
       ▼
   user:123
       │
       ▼
     Redis
```

Possible strategies:

* Local pre-filtering
* Hierarchical rate limiting
* Sharding where semantics allow it
* Dedicated Redis capacity
* Separate limits at different layers

However, blindly splitting one user's state across Redis keys changes the exact semantics of the limiter.

Document this tradeoff rather than pretending sharding is free.

---

# 29. Memory Management

A distributed rate limiter can create a large number of Redis keys.

For example:

```text
10 million users
×
one rate-limit key
=
10 million Redis keys
```

Therefore:

* Use TTLs.
* Remove inactive state.
* Monitor Redis memory.
* Use bounded data structures.
* Avoid storing unnecessary metadata.

For sliding windows, old timestamps must be removed aggressively.

---

# 30. Security

The rate limiter itself should not become an attack surface.

Consider:

* Authentication between clients and limiter
* TLS
* Redis authentication
* Network isolation
* Request-size limits
* Key validation
* Maximum configurable limits
* Protection against arbitrary-key flooding

For example, don't allow an unauthenticated client to generate millions of unique keys:

```text
key=random-1
key=random-2
key=random-3
...
key=random-10000000
```

This could cause Redis memory exhaustion.

---

# 31. Performance Considerations

The ideal request path should be:

```text
HTTP
 ↓
Validate
 ↓
Redis Lua
 ↓
Response
```

Avoid:

```text
HTTP
 ↓
multiple Redis round trips
 ↓
application-side calculations
 ↓
multiple Redis updates
 ↓
response
```

A single Lua invocation reduces network round trips and provides atomicity.

---

# 32. Project Milestones

## Milestone 1 — Basic Service

Implement:

```text
Go HTTP server
Redis connection
POST /v1/check
health endpoint
```

---

## Milestone 2 — Fixed Window

Implement:

```text
INCR
TTL
Lua
```

Add unit + integration tests.

---

## Milestone 3 — Sliding Window

Implement:

```text
Redis Sorted Set
ZREMRANGEBYSCORE
ZCARD
ZADD
```

inside Lua.

---

## Milestone 4 — Token Bucket

Implement:

```text
tokens
lastRefill
capacity
refillRate
requestCost
```

inside Lua.

This becomes the main implementation.

---

## Milestone 5 — Distributed Testing

Run:

```text
3 Go instances
+
Redis
+
load generator
```

Verify correctness under concurrent requests.

---

## Milestone 6 — Observability

Add:

```text
Prometheus
Grafana
structured logging
```

---

## Milestone 7 — Failure Handling

Test:

```text
Redis unavailable
Redis timeout
Redis restart
network latency
```

Implement the selected failure policy.

---

## Milestone 8 — Redis Cluster

Investigate:

```text
Redis Cluster
hash slots
hash tags
replication
failover
```

and document the implications.

---

# 33. Final Architecture

The completed system should look like:

```text
                         ┌───────────────────┐
                         │      Client       │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │   Load Balancer   │
                         └─────────┬─────────┘
                                   │
             ┌─────────────────────┼─────────────────────┐
             │                     │                     │
             ▼                     ▼                     ▼
       ┌────────────┐        ┌────────────┐        ┌────────────┐
       │ Go Node 1  │        │ Go Node 2  │        │ Go Node 3  │
       │ Stateless  │        │ Stateless  │        │ Stateless  │
       └──────┬─────┘        └──────┬─────┘        └──────┬─────┘
              │                     │                     │
              └─────────────────────┼─────────────────────┘
                                    │
                                    ▼
                           ┌─────────────────┐
                           │ Redis Cluster   │
                           │                 │
                           │ Shared State    │
                           │ Lua Scripts     │
                           │ TTL             │
                           └────────┬────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
             ┌──────────────┐                ┌──────────────┐
             │  Prometheus  │                │    Logs      │
             └──────┬───────┘                └──────────────┘
                    │
                    ▼
               ┌─────────┐
               │ Grafana │
               └─────────┘
```

---

# 34. System Design Concepts Demonstrated

This project should demonstrate more than just rate limiting.

### Distributed State

Why local state doesn't work when horizontally scaling.

### Atomicity

Why rate-limit decisions must be atomic.

### Concurrency

Handling thousands of simultaneous requests.

### Shared State

Redis as the authoritative state store.

### Failure Handling

What happens when Redis becomes unavailable.

### Horizontal Scaling

Adding more Go instances without changing correctness.

### Hot Keys

Understanding concentration of traffic on a single Redis key.

### Memory Management

TTL and cleanup of inactive identities.

### Consistency

Understanding the relationship between Redis state and rate-limit guarantees.

### Observability

Measuring whether the system actually behaves as designed.

### Performance

Understanding the cost of network round trips and Lua execution.

---

# 35. Recommended Final Demo

The strongest demo is:

```text
1. Start Redis

2. Start 3 Go rate-limiter instances

3. Put them behind a load balancer

4. Configure:

   capacity = 100
   refillRate = 10/sec

5. Send 10,000 concurrent requests

6. Show:
   - exactly how many were allowed
   - rejection rate
   - p50/p95/p99 latency
   - Redis latency

7. Kill one Go instance

8. Continue sending traffic

9. Show that rate limiting continues to work

10. Kill Redis

11. Demonstrate the configured failure policy

12. Restart Redis

13. Verify recovery
```

This gives you a concrete demonstration of:

```text
              DISTRIBUTED SYSTEM
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
   Correctness   Scalability   Reliability
       │             │             │
       ▼             ▼             ▼
    Lua/Redis    Stateless Go   Failure policy
```

---

# 36. Final Resume-Level Description

After completing the project, a concise project description could be:

> **Distributed Rate Limiter — Go, Redis, Lua, Docker, Prometheus**
>
> Built a horizontally scalable distributed rate-limiting service in Go using Redis as shared state, implementing Fixed Window, Sliding Window, and Token Bucket algorithms. Designed atomic Lua-based rate-limit operations to prevent race conditions across multiple service instances, added Redis-backed state expiration and failure handling, and benchmarked concurrent workloads with Prometheus/Grafana observability.

---

# 37. Implementation Priority

Don't over-engineer the first version.

Build in this exact order:

```text
                    START
                      │
                      ▼
               Go HTTP Server
                      │
                      ▼
                   Redis
                      │
                      ▼
               Fixed Window
                      │
                      ▼
                 Redis Lua
                      │
                      ▼
              Integration Tests
                      │
                      ▼
              Sliding Window
                      │
                      ▼
               Token Bucket
                      │
                      ▼
          Multiple Go Instances
                      │
                      ▼
                Load Testing
                      │
                      ▼
              Prometheus/Grafana
                      │
                      ▼
             Failure Handling
                      │
                      ▼
               Redis Cluster
                      │
                      ▼
                    DONE
```

**The core of the project should remain:**

```text
                 ┌───────────────┐
                 │   Go Nodes    │
                 │  stateless    │
                 └───────┬───────┘
                         │
                         ▼
                 ┌───────────────┐
                 │     Redis     │
                 │               │
                 │ Shared State  │
                 │      +        │
                 │ Lua Atomicity │
                 └───────────────┘
```

Everything else—load balancing, observability, failure handling, clustering, and benchmarking—exists to demonstrate how that core behaves as a **real distributed system**.
