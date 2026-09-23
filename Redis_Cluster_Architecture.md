# Architecture: Dwaarpal Distributed Rate Limiter

## 1. System Overview

Dwaarpal is a highly available, distributed rate-limiting service built in Go. It enforces rate limits across thousands of concurrent connections in a distributed environment without bottlenecks.

The architecture consists of two primary layers:
1. **Stateless Go API Nodes**: These handle incoming HTTP requests, provide metrics (Prometheus), and enforce rate limits using algorithms like Token Bucket, Sliding Window, and Fixed Window.
2. **Stateful Redis Cluster**: The authoritative, shared state store where counters and timestamps are stored.

By decoupling the state (Redis) from the application logic (Go), Dwaarpal achieves **horizontal scalability**. You can add as many Go nodes as needed behind a Load Balancer to handle incoming API traffic.

---

## 2. Eliminating the Single Point of Failure (SPOF)

In the initial design, a standalone Redis instance was used. While performant, this created a Single Point of Failure. If the Redis container crashed, the entire rate-limiting system would fail.

To build a production-grade system, Dwaarpal uses a **6-node Redis Cluster**:
- **3 Master Nodes** (Handling reads/writes for specific data partitions)
- **3 Replica Nodes** (1 hot-standby replica for each Master)

This setup provides two massive benefits: **Horizontal Scalability (Sharding)** and **High Availability (Replication)**.

---

## 3. Sharding & Client-Side Routing

Instead of storing all rate-limit data on one machine, Redis Cluster distributes the data. 

### The Hash Slots
Redis Cluster does not use traditional "consistent hashing" (like a ring where you can add/remove nodes and only affect adjacent keys). Instead, the keyspace is permanently divided into **16,384 Hash Slots**.
- Master 1: Slots 0 - 5460
- Master 2: Slots 5461 - 10922
- Master 3: Slots 10923 - 16383

### Client-Side Routing (go-redis)
When a request comes into Dwaarpal for a specific user (e.g., `user:123`), how does the Go application know which Master node to talk to? 

Dwaarpal uses the `go-redis` `UniversalClient`. This client handles the complex topology mapping automatically:
1. **Topology Discovery**: On startup, the client queries the cluster (using the `CLUSTER SLOTS` command) to download a map of which Master owns which slots.
2. **Key Hashing**: When we make a rate limit request, the client hashes the key using `CRC16(key) mod 16384` to find the exact slot.
3. **Direct Routing**: The client sends the Lua script execution **directly** to the Master node that owns that slot.
4. **Topology Updates**: If we add more nodes to the cluster, slots will migrate. If the Go client sends a request to the wrong node, the node responds with a `MOVED` error. The client intercepts this, silently updates its internal slot map, and retries on the correct node without the application ever knowing.

---

## 4. Lua Script Atomicity

Rate limiting requires checking a value (e.g., current tokens) and updating it (e.g., consuming a token). Doing this in two separate network trips causes race conditions when thousands of concurrent requests hit the system.

Dwaarpal solves this by shipping the algorithm logic directly to Redis using **Lua Scripts**. Redis executes Lua scripts atomically—no other operations can run while the script is executing. 

**Cluster Compatibility**: Redis Cluster requires that all keys modified in a single Lua script execution reside on the same hash slot. Because Dwaarpal's algorithms only evaluate a **single key** (`KEYS[1]`) per request, our Lua scripts are natively compatible with Redis Cluster routing.

---

## 5. High Availability & Failover

What happens if Master 2's server physically burns down?

1. **Gossip Protocol**: The cluster nodes constantly ping each other. When Master 2 stops responding, the surviving nodes agree that it has failed.
2. **Election**: The cluster holds an election and automatically promotes **Replica 2** to become the new Master for slots 5461 - 10922.
3. **Healing**: The cluster is now fully operational again. When the dead node is restored, it will automatically rejoin as a Replica and sync the data.

### Fail Closed Protection
During the brief 3-5 second window while the cluster is electing the new Master, requests routed to those specific slots will fail. 

Dwaarpal employs a **Fail Closed** strategy with a strict **50ms timeout**. If the Redis call takes longer than 50ms, Dwaarpal aborts the request and immediately returns a `503 Service Unavailable` with an `"allowed": false` JSON payload. 

This protects the Go application instances from hanging indefinitely and consuming all connection pool resources, ensuring the overall API remains stable even when the state store is struggling.

---

## 6. Observability

Dwaarpal is fully instrumented with Prometheus metrics.
- `rate_limit_requests_total`: Tracks overall throughput and allowed/rejected ratios.
- `rate_limit_decision_latency_seconds`: A histogram tracking the execution speed of the Lua scripts inside the Redis Cluster.
- `rate_limit_redis_errors_total`: Tracks fail-closed events.

These metrics allow operators to visualize exactly how the distributed cluster is performing in real-time via Grafana.
