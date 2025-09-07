#!/usr/bin/env bash
set -euo pipefail
TOPIC="${1:-prepared_data_json}"
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ${TOPIC} --partitions 6 --replication-factor 1"
echo "Topic '${TOPIC}' ensured."
