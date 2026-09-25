#!/bin/bash
set -e

echo "============================================="
echo " DWAARPAL BARE-METAL CLUSTER SCALING BENCHMARK "
echo "============================================="

cd "$(dirname "$0")/.."

echo "Building Dwaarpal native binary for macOS..."
go build -o dwaarpal-api cmd/server/main.go

echo "Starting clean Redis Cluster environment in Docker..."
docker-compose -f deployments/docker-compose.standalone.yml down -v 2>/dev/null || true
docker-compose -f deployments/docker-compose.benchmark.yml down -v 2>/dev/null || true
docker-compose -f deployments/docker-compose.cluster.yml down -v 2>/dev/null || true

# Booting the cluster with IP=127.0.0.1 so the native Mac OS client can resolve the MOVED redirects
cat <<EOF > deployments/docker-compose.native_cluster.yml
version: '3.8'
services:
  redis-cluster:
    image: grokzen/redis-cluster:latest
    container_name: dwaarpal-native-redis-cluster
    environment:
      - INITIAL_PORT=7010
      - IP=127.0.0.1
    ports:
      - "7010:7010"
      - "7011:7011"
      - "7012:7012"
      - "7013:7013"
      - "7014:7014"
      - "7015:7015"
EOF

docker-compose -f deployments/docker-compose.native_cluster.yml up -d >&2

echo "Waiting 10 seconds for Redis Cluster to form and negotiate slots..."
sleep 10

echo "Disabling Redis protected mode on all cluster nodes..."
for port in {7010..7015}; do
    docker exec dwaarpal-native-redis-cluster redis-cli -p $port CONFIG SET protected-mode no >/dev/null 2>&1 || true
done
sleep 2

REPLICAS=(1 2 4 8)
BASE_THROUGHPUT=0
PIDS=()

cleanup() {
    echo "Cleaning up native processes..." >&2
    for pid in "${PIDS[@]}"; do
        kill -9 $pid 2>/dev/null || true
    done
    PIDS=()
}
trap cleanup EXIT

for N in "${REPLICAS[@]}"; do
    cleanup
    
    echo "Starting $N native Go processes..." >&2
    for (( i=1; i<=N; i++ )); do
        PORT=$((8080 + i))
        PORT=$PORT GRPC_PORT=$((50050 + i)) REDIS_ADDRESS="127.0.0.1:7010,127.0.0.1:7011,127.0.0.1:7012" ./dwaarpal-api >/dev/null 2>&1 &
        PIDS+=($!)
    done
    
    sleep 3

    echo "Running native k6 load test for $N node(s)..." >&2
    NUM_NODES=$N k6 run --summary-export=benchmark/native_cluster_results_${N}.json benchmark/k6_native_load_test.js >&2 || true

    # Extract base throughput if N=1
    if [ "$N" -eq 1 ]; then
        BASE_THROUGHPUT=$(python3 -c "import json; print(json.load(open('benchmark/native_cluster_results_1.json'))['metrics']['http_reqs']['rate'])")
    fi
done

echo ""
echo "| Nodes | Req/s | p50 (ms) | p95 (ms) | Efficiency |"
echo "|-------|-------|----------|----------|------------|"

for N in "${REPLICAS[@]}"; do
    N=$N BASE_THROUGHPUT=$BASE_THROUGHPUT python3 -c "
import json
import os

N = int(os.environ['N'])
BASE_THROUGHPUT = float(os.environ['BASE_THROUGHPUT'])

try:
    with open(f'benchmark/native_cluster_results_{N}.json') as f:
        data = json.load(f)
        
        rate = data['metrics']['http_reqs']['rate']
        dur = data['metrics']['http_req_duration']
        p50 = dur.get('med', 0)
        p95 = dur.get('p(95)', 0)
        
        base_rate = BASE_THROUGHPUT
        if N == 1:
            efficiency = '100.0%'
        else:
            ideal = base_rate * N
            efficiency = f'{(rate / ideal) * 100:.1f}%'
            
        print(f'| {N} | {rate:,.0f} | {p50:.2f} | {p95:.2f} | {efficiency} |')
except Exception as e:
    print(f'| {N} | ERROR | ERROR | ERROR | ERROR |')
"
done

echo "============================================="
echo " Benchmark Complete. "
echo "============================================="

docker-compose -f deployments/docker-compose.native_cluster.yml down -v >&2 || true
rm deployments/docker-compose.native_cluster.yml
