<div align="center">
  <h1>🛡️ Dwaarpal 🛡️</h1>
  <p><b>A highly available, horizontally scalable, dual-stack (HTTP & gRPC) Distributed Rate Limiter built in Go.</b></p>
  <p>
    <img src="https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go" alt="Go Version" />
    <img src="https://img.shields.io/badge/Redis-Cluster_Ready-DC382D?style=for-the-badge&logo=redis" alt="Redis Cluster" />
    <img src="https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge" alt="License" />
  </p>
</div>

---

## 📖 Table of Contents
- [What is Dwaarpal?](#-what-is-dwaarpal)
- [The Architecture of Scale](#-the-architecture-of-scale)
  - [1. Completely Stateless (Horizontal Scaling)](#1-completely-stateless-horizontal-scaling)
  - [2. The Race Condition Problem (Lua Atomicity)](#2-the-race-condition-problem-lua-atomicity)
  - [3. Eliminating the SPOF (Sharding & Replication)](#3-eliminating-the-spof-sharding--replication)
  - [4. True Redis Pipelining & The Evaluation Flow](#4-true-redis-pipelining--the-evaluation-flow)
  - [5. Production-Ready Safety Mechanisms](#5-production-ready-safety-mechanisms)
  - [6. Supported Algorithms](#6-supported-algorithms)
- [🚀 Quickstart Deployments](#-quickstart-deployments)
  - [Option 1: Standalone (Local Development)](#option-1-standalone-local-development)
  - [Option 2: Production Cluster Simulation](#option-2-production-cluster-simulation)
- [🏢 Enterprise Deployment (BYOI)](#-enterprise-deployment-byoi)
- [🔌 API Usage (Dual-Stack Multi-Key)](#-api-usage-dual-stack-multi-key)
- [📊 Observability](#-observability)

---

## 💡 What is Dwaarpal?
**Dwaarpal** (Sanskrit for *Gatekeeper*) is an enterprise-grade rate-limiting microservice. It is designed to sit behind your API Gateway or internal Load Balancer and instantly decide whether an incoming request should be allowed or rejected based on strict algorithmic quotas.

It supports **Token Bucket**, **Fixed Window**, and **Sliding Window** algorithms, and exposes both high-performance **gRPC** and standard **HTTP/REST** interfaces.

---

## 🧠 The Architecture of Scale

Building a rate limiter is easy. Building a *distributed* rate limiter that handles massive multi-key evaluation concurrently across a fleet of microservices without melting your database is extremely difficult. Here is how Dwaarpal solves the hardest problems in distributed systems:

### 1. Completely Stateless (Horizontal Scaling)
The Dwaarpal Go API nodes hold absolutely **zero state** about how many requests a user has made. They are pure computation nodes. 

This means you can spin up 1,000 Dwaarpal containers side-by-side behind an AWS ALB or Kubernetes LoadBalancer. They do not need to talk to each other, sync data, or care which container handles which request. You can horizontally scale your rate-limiting throughput infinitely just by adding more Go nodes.

### 2. The Race Condition Problem (Lua Atomicity)
**The Problem**: If a user has exactly 1 token left in their quota, and they maliciously blast your API with 50 parallel requests at the exact same millisecond, those requests will hit 50 different Dwaarpal nodes simultaneously. If the nodes read the Redis value ("Tokens = 1"), subtract it in Go ("Tokens = 0"), and write it back, all 50 nodes would see "Tokens = 1" and allow the request. You would have allowed 50 requests instead of 1.

**The Solution**: Dwaarpal completely bypasses this by shipping the algorithmic logic directly to the database via **Lua Scripts**. When the 50 nodes receive the requests, they fire a Lua script at the Redis Master. Because the Redis core engine is single-threaded, it executes these Lua scripts **atomically** in a queue. Exactly 1 script will read the token and subtract it. The other 49 will instantly fail. Zero race conditions.

### 3. Eliminating the SPOF (Sharding & Replication)
Using a single Redis node creates a massive Single Point of Failure (SPOF) and a network bottleneck. Dwaarpal is engineered to run against a **Redis Cluster**.

- **Sharding via Hash Slots**: The Redis Cluster divides your entire database into exactly 16,384 "Hash Slots" distributed across multiple physical shards.
- **Client-Side Routing**: Dwaarpal natively calculates `CRC16(key) % 16384` and routes the Lua script execution **directly over the network to the exact physical shard that owns the data**.
- **High Availability & Leader Election**: If a Master node physically burns down, the cluster nodes use a continuous **Gossip Protocol** to detect the failure. The surviving nodes immediately hold a **Leader Election** and automatically promote a hot-standby Replica to become the new Master for those specific hash slots. During this brief election window, Dwaarpal seamlessly relies on its Fail-Closed protection.

### 4. True Redis Pipelining & The Evaluation Flow
Dwaarpal allows API Gateways to submit a batch array of rate limits in a single HTTP/gRPC request (e.g., limit by `IP` AND `User` AND `Route`). Evaluating multiple keys across a distributed cluster requires an optimal, zero-latency pipeline flow:

1. **Phase 1: Boot State & Pre-Warming**
   Dwaarpal iterates through the cluster topology, running `SCRIPT LOAD` on every shard. The scripts are cached in Redis RAM, and Go stores the SHA1 hashes.
2. **Phase 2: L1 Blackbox Intercept (O(1) Loop)**
   When a batch of keys arrives, Dwaarpal loops through them locally in RAM. If **EVEN ONE** key is found in the local L1 Penalty Box, the entire payload is instantly short-circuited and rejected with HTTP 429. **Exactly 0 network calls are made.**
3. **Phase 3: Pipeline Queueing & Execution**
   If the L1 Cache is bypassed, Dwaarpal buffers the `EVALSHA` commands into a Redis Pipeline in memory. The `go-redis` client mathematically groups the keys by their physical shards and sends concurrent, bundled TCP packets. Multiple keys mapping to the same shard are executed in a single network roundtrip.
4. **Phase 4: Parsing & The "All-or-Nothing" Blackbox**
   Dwaarpal extracts the pipeline results. If *any* key in the batch failed, Dwaarpal identifies **every single failed key** and independently adds all of them to the local L1 Penalty Box. It then aggregates the data, returning the most restrictive `RetryAfter` time to the Gateway.

### 5. Production-Ready Safety Mechanisms

#### The NOSCRIPT Dynamic Self-Healing
If a Redis Node crashes and reboots, its RAM is wiped, meaning the Lua Scripts disappear. Executing a pipeline will result in a `NOSCRIPT` error. Instead of dropping the request, Dwaarpal's engine intercepts this error, dynamically fires a background thread to reload the scripts into the cluster, and seamlessly retries the pipeline execution without dropping the user's connection.

#### Fail-Closed Protection
Dwaarpal employs a strict **Fail-Closed Strategy**. If a pipeline takes longer than the configured `REDIS_TIMEOUT_MS`, Dwaarpal instantly aborts the request, sheds the load, and returns a `500 Internal Error` (reverting to failsafe mode). Your infrastructure stays completely healthy.

#### Two-Tier Rate Limiting (The L1 Blackbox Cache)
To protect the Redis cluster from devastating DDoS attacks that attempt to breach rate limits concurrently, Dwaarpal implements an aggressive **Two-Tier Negative Caching** architecture directly in Go memory.
1. **Surface Area Reduction**: If a botnet blasts 100,000 requests/sec at a key, the API Gateway distributes the load across your `N` horizontally scaled Dwaarpal pods. The *first* request on each pod calls Redis, which rejects it. The pod instantly populates its local "Blackbox" L1 cache. The remaining 99,999 requests are intercepted locally in RAM and rejected instantly. This mathematical optimization guarantees the absolute maximum number of Redis calls an attacker can trigger is equal to `N` (the number of Dwaarpal pods).
2. **Composite Key Safety**: To prevent a user from being globally blacklisted just for breaching one specific limit, the L1 Cache mathematically partitions keys by creating a composite constraint signature: `Algorithm:Key:Limit:Window` (e.g. `TOKEN_BUCKET:user:123:10:60`).
3. **High-Concurrency LRU**: The cache is built on `hashicorp/golang-lru/v2`, famously used in enterprise tools like Consul. It employs highly sharded `sync.RWMutex` locks to guarantee thread safety and prevent deadlock cascades, even when 10,000 goroutines attempt to read the cache simultaneously.
4. **Lazy TTL Eviction**: To prevent CPU waste from background sweeping threads, the cache utilizes a "Lazy Eviction" pattern. The L1 stores the exact global UTC expiry time (`time.Now().UTC().Add(retryAfter)`). On every read, it evaluates if the time has passed. Furthermore, the cache size is strictly bounded (`L1_CACHE_SIZE=100000`), guaranteeing that RAM usage will never exceed ~15MB. When the cache fills up, the Least Recently Used keys are safely evicted to make room, making Dwaarpal perfectly immune to Out-Of-Memory (OOM) crashes during randomized DDoS attacks.

#### Kubernetes Graceful Shutdown
When Kubernetes scales down a pod or deploys a new version, it sends a `SIGTERM` signal to the process. Dwaarpal natively traps this signal and executes a **Graceful Shutdown**:
1. Stops accepting *new* HTTP and gRPC connections.
2. Waits for all active, in-flight requests to finish processing (up to 15 seconds).
3. Safely disconnects the Redis Connection Pool to prevent ghost connections on the cluster.
4. Exits cleanly with code 0.

#### Configurable DDoS Batch Protection
To prevent malicious clients from attempting memory exhaustion via infinite JSON arrays, Dwaarpal enforces a `MAX_BATCH_SIZE`. Any payload exceeding this size is instantly dropped with a `400 Bad Request`.

### 6. Supported Algorithms
Dwaarpal implements five mathematically distinct rate-limiting algorithms natively in Lua:
- **Token Bucket**: Perfect for smoothing out bursts. Tokens are added to the bucket at a steady rate; requests consume tokens.
- **Leaky Bucket**: Enforces a strict output rate. Great for traffic shaping and preventing downstream overwhelming.
- **Fixed Window**: The simplest approach. Counts requests within discrete time blocks. High performance, but suffers from edge-case bursting.
- **Sliding Window Log**: The most mathematically accurate algorithm, implemented using Redis Sorted Sets (`ZSET`). Logs exact timestamps to smooth out traffic continuously.
- **Sliding Window Counter**: A highly memory-efficient approximation of the Sliding Window Log. Perfect for massive scale where `ZSET` memory overhead is unacceptable.

---

## 🚀 Quickstart Deployments

Dwaarpal provides pre-configured Docker Compose files to get you up and running instantly.

### Option 1: Standalone (Local Development)
The easiest way to test Dwaarpal locally is using the standalone deployment. This spins up the Go API alongside a single Redis node, Prometheus, and Grafana.

```bash
docker-compose -f deployments/docker-compose.standalone.yml up -d --build
```

### Option 2: Production Cluster Simulation
To simulate a real-world distributed environment, Dwaarpal includes a fully functional **6-node Redis Cluster** (3 Masters, 3 Replicas) deployment. 

Run the entire cluster stack (API + Redis Cluster + Observability):
```bash
docker-compose -f deployments/docker-compose.cluster.yml up -d --build
```

### Option 3: Load-Balanced Benchmark Architecture
To test horizontal scaling and Nginx load balancing over multiple Dwaarpal replicas hitting a single Redis node, use the benchmark architecture. This exposes Nginx on port `80`.

```bash
docker-compose -f deployments/docker-compose.benchmark.yml up -d --scale dwaarpal-api=3 --build
```

---

## 🏢 Enterprise Deployment (BYOI)

Dwaarpal is designed for **Bring Your Own Infrastructure (BYOI)**. The Go application natively supports connecting to standalone Redis, Redis Sentinel, or managed clusters like AWS ElastiCache and Upstash (via TLS `rediss://` parsing).

If a Platform Engineering team wants to deploy Dwaarpal in their private cloud against a massive managed ElastiCache cluster, they simply pull the Docker image and inject the configuration endpoint:

```bash
docker run -d \
  -p 8080:8080 \
  -p 50051:50051 \
  -e PORT="8080" \
  -e GRPC_PORT="50051" \
  -e REDIS_ADDRESS="clustercfg.production-redis.us-east-1.cache.amazonaws.com:6379" \
  -e REDIS_TIMEOUT_MS="50" \
  ghcr.io/pratham-developer/dwaarpal:latest
```

---

## 🔌 API Usage (Dual-Stack Multi-Key)

Dwaarpal processes batches of descriptors in a single call. You can rate limit by IP, User ID, and API Route concurrently.

### HTTP (REST) - Port 8080
```bash
curl -X POST http://localhost:8080/v1/check \
     -H "Content-Type: application/json" \
     -d '[
           {"key": "IP:192.168.1.1", "algorithm": "TOKEN_BUCKET", "limit": 100, "window": 60},
           {"key": "USER:456", "algorithm": "TOKEN_BUCKET", "limit": 1000, "window": 3600}
         ]'
```
**Response (Allowed)**:
```json
{
  "allowed": true,
  "remaining": 99,
  "retry_after": 0,
  "reset_at": "2026-09-24T16:22:09Z"
}
```
**Response (Rejected)**:
```json
{
  "allowed": false,
  "failed_key": "USER:456",
  "remaining": 0,
  "retry_after": 3600000,
  "reset_at": "2026-09-24T17:22:09Z"
}
```

### gRPC - Port 50051
*(Note: Dwaarpal implements **gRPC Reflection**, so tools like Postman and grpcurl can automatically discover the `RateLimiterService` schema.)*

```bash
grpcurl -plaintext -d '{
  "descriptors": [
    {"key": "IP:192.168.1.1", "algorithm": "TOKEN_BUCKET", "limit": 100, "window": 60}
  ]
}' localhost:50051 ratelimit.RateLimiterService/CheckRateLimit
```

---

## 📊 Observability

Dwaarpal is built for Day-2 operations. It exposes a `/metrics` endpoint on port `8080` that is scraped by Prometheus.

**Available Metrics:**
- `rate_limit_requests_total`: Tracks overall throughput and allowed/rejected ratios.
- `rate_limit_decision_latency_seconds`: A histogram tracking the execution speed of the Lua scripts inside Redis.
- `rate_limit_redis_errors_total`: Tracks timeout and fail-closed events.

---

## ⚙️ Configuration (Environment Variables)

Dwaarpal is configured strictly through environment variables to align with 12-Factor App methodology.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8080` | The HTTP REST Port. |
| `GRPC_PORT` | `50051` | The gRPC Port. |
| `REDIS_ADDRESS` | `localhost:6379` | Supports standalone (`ip:port`), cluster (`ip1:port1,ip2:port2`), or TLS (`rediss://...`) |
| `REDIS_TIMEOUT_MS` | `50` | Maximum allowed latency per pipeline before triggering Fail-Closed abortion. |
| `L1_CACHE_SIZE` | `100000` | Max items in the local RAM Penalty Box. (~15MB overhead at max capacity). |
| `MAX_BATCH_SIZE` | `100` | Max number of keys allowed in a single payload. Protects against memory exhaustion. |

---

## 🧪 Testing

The repository contains rigorous integration and load testing suites.

To run the integration tests (requires a running Redis instance):
```bash
docker-compose -f deployments/docker-compose.standalone.yml up -d
go test ./tests/integration/... -v
```

To run the intense DDoS simulation load test (tests race conditions and atomicity):
```bash
go test ./tests/load/... -v -run TestConcurrentDDoSMitigation
```

---

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
