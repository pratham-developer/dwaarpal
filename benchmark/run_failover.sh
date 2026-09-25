#!/bin/bash
set -e

echo "============================================="
echo " DWAARPAL REDIS FAILOVER BENCHMARK "
echo "============================================="

cd "$(dirname "$0")/.."

echo "Starting clean standalone environment..."
docker-compose -f deployments/docker-compose.benchmark.yml down -v 2>/dev/null || true
docker-compose -f deployments/docker-compose.standalone.yml down -v 2>/dev/null || true
docker-compose -f deployments/docker-compose.standalone.yml up -d >&2

echo "Waiting for Dwaarpal to boot..."
until curl -s http://localhost:8080/health > /dev/null; do
    sleep 2
done
sleep 2

# Start prober in background
go run benchmark/failover_prober.go &
PROBER_PID=$!

echo "[T0] Failover script: Let prober run for 5 seconds to establish baseline..."
sleep 5

echo "[T1] Injecting Hard Redis Failure (docker kill dwaarpal-redis)..."
docker kill dwaarpal-redis > /dev/null

echo "[T1+5s] Waiting 5 seconds before recovery..."
sleep 5

echo "[T2] Simulating Redis Recovery (docker start dwaarpal-redis)..."
docker start dwaarpal-redis > /dev/null

echo "[T2+10s] Allowing 10 seconds for API to reconnect and heal scripts..."
sleep 10

echo "Stopping prober..."
kill $PROBER_PID || true

echo "============================================="
echo " Failover Benchmark Results "
echo "============================================="
cat /tmp/dwaarpal_failover_log.txt
echo "============================================="
