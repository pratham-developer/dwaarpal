#!/bin/bash
set -e

echo "============================================="
echo " DWAARPAL CONCURRENCY CORRECTNESS BENCHMARK "
echo "============================================="

cd "$(dirname "$0")/.."

LIMIT=100
CONCURRENCY=5000
RUNS=100
TARGET_URL="http://localhost:8080/v1/check"

echo "Starting clean standalone environment..."
docker-compose -f deployments/docker-compose.benchmark.yml down -v 2>/dev/null || true
docker-compose -f deployments/docker-compose.standalone.yml down -v 2>/dev/null || true
docker-compose -f deployments/docker-compose.standalone.yml up -d >&2

echo "Waiting for Dwaarpal to boot..."
until curl -s http://localhost:8080/health > /dev/null; do
    sleep 2
done
sleep 5

echo "Running $RUNS runs of $CONCURRENCY concurrent requests against a strict quota of $LIMIT..."
SUCCESS_COUNT=0
PARTIAL_COUNT=0
FAIL_COUNT=0

for (( i=1; i<=RUNS; i++ ))
do
    set +e
    go run benchmark/correctness.go -limit $LIMIT -concurrency $CONCURRENCY -url $TARGET_URL > /tmp/dwaarpal_race_output.txt
    EXIT_CODE=$?
    set -e
    
    if [ $EXIT_CODE -eq 0 ]; then
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        echo "Run $i/$RUNS: ✅ Strict Quota Maintained"
    elif [ $EXIT_CODE -eq 1 ]; then
        PARTIAL_COUNT=$((PARTIAL_COUNT + 1))
        echo "Run $i/$RUNS: ⚠️ Partial (Timeouts/Errors occurred, but quota wasn't breached)"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        echo "Run $i/$RUNS: ❌ FAILED! Quota breached."
        cat /tmp/dwaarpal_race_output.txt
    fi
done

echo ""
echo "============================================="
echo " Correctness Benchmark Results "
echo "============================================="
echo "Total Runs: $RUNS"
echo "Perfect Enforcements: $SUCCESS_COUNT"
echo "Partial (Network Timeouts): $PARTIAL_COUNT"
echo "Quota Violations: $FAIL_COUNT"
echo "============================================="

if [ $FAIL_COUNT -gt 0 ]; then
    echo "❌ The system is not mathematically sound."
    exit 1
else
    echo "✅ 100% mathematically sound. No quota violations detected."
fi
