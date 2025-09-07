Place the ClickHouse Kafka Sink Connector here.

Example:
connect-plugins/
  clickhouse-kafka-connect/
    clickhouse-kafka-connect-<version>.jar
    (plus any bundled dependency jars if required)

Restart `kafka-connect` service after placing the JARs:
  docker compose restart kafka-connect
