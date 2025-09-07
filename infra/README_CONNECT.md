# Kafka → ClickHouse (JSON) via ClickHouse Kafka Sink

## Prereq

1. Download the ClickHouse Kafka Sink connector JARs and place them under:
   `infra/connect-plugins/clickhouse-kafka-connect/`
   Then restart `kafka-connect` container.

## Start stack

```bash
cd infra
docker compose up -d

Prepare ClickHouse & Kafka
# ensure DB/table auto-created from /sql on first start
docker exec clickhouse clickhouse-client -q "SELECT currentDatabase()"
bash scripts/create_topic.sh
bash scripts/produce_sample_json.sh

Register connector
bash scripts/register_connector.sh

Verify ingestion
docker exec -it clickhouse clickhouse-client -q "SELECT count(), min(eventTime), max(eventTime) FROM hades.prepared_data_json"
docker exec -it clickhouse clickhouse-client -q "SELECT * FROM hades.prepared_data_json ORDER BY eventTime LIMIT 10"

Notes

JSON is schemaless; unknown fields are ignored via input_format_skip_unknown_fields=1.

Timestamps parsed with date_time_input_format=best_effort (supports strings like YYYY-MM-DD hh:mm:ss.SSS).

If your real topic name differs, adjust:

scripts/create_topic.sh

scripts/produce_sample_json.sh

connectors/clickhouse-sink.json (topics + topic2TableMap)
```
