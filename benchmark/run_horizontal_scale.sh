#!/bin/bash
set -e

echo "============================================="
echo " DWAARPAL HORIZONTAL SCALING BENCHMARK "
echo "============================================="

# Ensure we're in the right directory
cd "$(dirname "$0")/.."

# Array of replicas to test
REPLICAS=(1 2 4 8)

echo "| Nodes | Req/s | p50 (ms) | p95 (ms) | p99 (ms) | Efficiency |"
echo "|-------|-------|----------|----------|----------|------------|"

BASE_THROUGHPUT=0

for N in "${REPLICAS[@]}"; do
    echo "Stopping existing containers..." >&2
    docker-compose -f deployments/docker-compose.standalone.yml down -v 2>/dev/null || true
    docker-compose -f deployments/docker-compose.benchmark.yml down -v 2>/dev/null || true

    echo "Starting cluster with $N API node(s)..." >&2
    docker-compose -f deployments/docker-compose.benchmark.yml up --scale dwaarpal-api=$N -d >&2

    echo "Waiting for Nginx load balancer to report healthy..." >&2
    until curl -s http://localhost/health > /dev/null; do
        sleep 2
    done
    # Extra sleep to let things stabilize
    sleep 5

    echo "Running k6 load test for $N node(s)..." >&2
    # Run k6 and export summary, ignore threshold exit codes
    k6 run --summary-export=benchmark/results_${N}.json benchmark/k6_load_test.js >&2 || true

    # Parse results using Python
    python3 -c "
import json
import sys

try:
    with open('benchmark/results_${N}.json') as f:
        data = json.load(f)
        
        rate = data['metrics']['http_reqs']['rate']
        dur = data['metrics']['http_req_duration']
        p50 = dur.get('med', 0)
        p95 = dur.get('p(95)', 0)
        p99 = dur.get('p(99)', 0)
        
        # Read base throughput for efficiency calculation
        base_rate = $BASE_THROUGHPUT
        if base_rate == 0:
            efficiency = '100.0%'
        else:
            ideal = base_rate * $N
            efficiency = f'{(rate / ideal) * 100:.1f}%'
            
        print(f'| {$N} | {rate:,.0f} | {p50:.2f} | {p95:.2f} | {p99:.2f} | {efficiency} |')
except Exception as e:
    print(f'| {$N} | ERROR | ERROR | ERROR | ERROR | ERROR |')
"

    # Set base throughput if N=1
    if [ "$N" -eq 1 ]; then
        BASE_THROUGHPUT=$(python3 -c "import json; print(json.load(open('benchmark/results_1.json'))['metrics']['http_reqs']['rate'])")
    fi
done

echo "============================================="
echo " Benchmark Complete. "
echo "============================================="
