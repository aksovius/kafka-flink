#!/usr/bin/env bash
set -euo pipefail
TOPIC="${1:-auth_dsdauthenEnrich}"
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic ${TOPIC} --partitions 12 --replication-factor 1"
echo "Production topic '${TOPIC}' ensured."
