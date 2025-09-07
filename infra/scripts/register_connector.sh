#!/usr/bin/env bash
set -euo pipefail
CFG="${1:-infra/connectors/clickhouse-sink.json}"
curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @"${CFG}"
echo
echo "Connector submitted. Inspect at http://localhost:8083/connectors/clickhouse-sink-prepared-json/status"
