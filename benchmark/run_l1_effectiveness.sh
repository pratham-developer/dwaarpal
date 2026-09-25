#!/bin/bash
set -e

echo "============================================="
echo " DWAARPAL L1 CACHE EFFECTIVENESS BENCHMARK "
echo "============================================="

cd "$(dirname "$0")/.."

# Restart the standalone environment to ensure a clean state
echo "Starting clean standalone environment..." >&2
docker-compose -f deployments/docker-compose.benchmark.yml down -v 2>/dev/null || true
docker-compose -f deployments/docker-compose.standalone.yml down -v 2>/dev/null || true
docker-compose -f deployments/docker-compose.standalone.yml up -d >&2

echo "Waiting for Dwaarpal to boot..." >&2
until curl -s http://localhost:8080/health > /dev/null; do
    sleep 2
done
sleep 5

HIT_RATES=(0 25 50 75 90 99)

echo "| L1 Hit Rate | Req/s | p50 (ms) | p95 (ms) | Redis Ops/sec |"
echo "|-------------|-------|----------|----------|---------------|"

for HR in "${HIT_RATES[@]}"; do
    echo "Running k6 load test for ${HR}% Hit Rate..." >&2
    
    # We must change the URL to port 8080 since we are using standalone instead of Nginx
    # We can use sed or just pass an ENV var to k6!
    # Ah! k6_load_test.js hardcodes http://localhost/v1/check
    # We'll just temporarily export it to the API URL if we modify the k6 script
    
    TARGET_URL=http://localhost:8080/v1/check HIT_RATE=$HR k6 run --summary-export=benchmark/l1_results_${HR}.json benchmark/k6_load_test.js >&2 || true
    
    # Parse results
    python3 -c "
import json

try:
    with open('benchmark/l1_results_${HR}.json') as f:
        data = json.load(f)
        
        rate = data['metrics']['http_reqs']['rate']
        dur = data['metrics']['http_req_duration']
        p50 = dur.get('med', 0)
        p95 = dur.get('p(95)', 0)
        
        # Redis Ops/sec = Total Req/s * (100 - Hit Rate) / 100
        redis_ops = rate * ((100 - $HR) / 100.0)
        
        print(f'| {$HR}% | {rate:,.0f} | {p50:.2f} | {p95:.2f} | {redis_ops:,.0f} |')
except Exception as e:
    print(f'| {$HR}% | ERROR | ERROR | ERROR | ERROR |')
"
done

echo "============================================="
echo " L1 Benchmark Complete. "
echo "============================================="
