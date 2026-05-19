#!/bin/bash
# FlinkTech - Start Everything Script
# Starts all containers, runs load test, and starts metrics bridge

set -e

echo "========================================="
echo "  FlinkTech Production Prototype"
echo "========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running. Please start Docker first."
    exit 1
fi

# Start all containers
echo "1. Starting all containers..."
docker-compose up -d

# Wait for services to be healthy
echo "2. Waiting for services to be healthy..."
sleep 15

# Check Flink job status
echo "3. Checking Flink job status..."
JOB_STATE=$(curl -s http://localhost:8081/jobs/overview | python3 -c "import sys,json; print(json.load(sys.stdin)['jobs'][0]['state'])" 2>/dev/null || echo "UNKNOWN")
echo "   Flink Job State: $JOB_STATE"

if [ "$JOB_STATE" != "RUNNING" ]; then
    echo "   WARNING: Job is not running yet. Waiting 10 more seconds..."
    sleep 10
fi

# Start metrics bridge in background
echo "4. Starting metrics bridge..."
if pgrep -f "metrics_bridge.py" > /dev/null; then
    echo "   Metrics bridge already running"
else
    python3 scripts/metrics_bridge.py &
    echo "   Metrics bridge started (PID: $!)"
fi

# Run load test
echo "5. Running load test..."
python3 scripts/load_test.py --quick &
LOAD_TEST_PID=$!

echo ""
echo "========================================="
echo "  Services Running:"
echo "========================================="
echo "  Flink Dashboard:  http://localhost:8081"
echo "  Prometheus:       http://localhost:9090"
echo "  Grafana:          http://localhost:3000 (admin/admin)"
echo "  Pushgateway:      http://localhost:9091"
echo "========================================="
echo ""
echo "  Load test running in background (PID: $LOAD_TEST_PID)"
echo "  Metrics bridge running in background"
echo ""
echo "  To stop everything: docker-compose down"
echo "  To view Flink logs: docker logs flinktech-taskmanager-1 -f"
echo "  To run load test: python3 scripts/load_test.py --quick"
echo "========================================="

# Wait for load test to finish
wait $LOAD_TEST_PID 2>/dev/null || true

echo ""
echo "Load test completed. Check Grafana at http://localhost:3000"
echo "Dashboard: FlinkTech Monitoring Dashboard"
