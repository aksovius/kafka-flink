#!/usr/bin/env bash
set -euo pipefail

echo "=== Kafka → ClickHouse End-to-End Test ==="

# Start infrastructure
echo "1. Starting Docker Compose..."
cd "$(dirname "$0")/.."
docker compose up -d

# Wait for services to be ready
echo "2. Waiting for services to be ready..."
sleep 30

# Check ClickHouse
echo "3. Checking ClickHouse..."
docker exec clickhouse clickhouse-client -q "SELECT currentDatabase()"

# Create test topic
echo "4. Creating test topic..."
bash scripts/create_topic.sh

# Produce test data
echo "5. Producing test data..."
bash scripts/produce_sample_json.sh

# Register connector
echo "6. Registering ClickHouse connector..."
bash scripts/register_connector.sh

# Wait for processing
echo "7. Waiting for data processing..."
sleep 20

# Verify data in ClickHouse
echo "8. Verifying data in ClickHouse..."
echo "Count and time range:"
docker exec -it clickhouse clickhouse-client -q "SELECT count(), min(eventTime), max(eventTime) FROM hades.prepared_data_json"

echo "Sample data:"
docker exec -it clickhouse clickhouse-client -q "SELECT * FROM hades.prepared_data_json ORDER BY eventTime LIMIT 5"

echo "=== Test completed successfully! ==="
echo "Kafka-UI available at: http://localhost:8080"
echo "ClickHouse available at: http://localhost:8123"
