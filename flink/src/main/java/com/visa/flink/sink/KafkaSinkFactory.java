package com.visa.flink.sink;

import com.visa.flink.model.ConsumerGenericRecord;
import com.visa.flink.serialization.JsonMapValueSerializer;
import com.visa.flink.serialization.AvroValueSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class KafkaSinkFactory {

    private KafkaSinkFactory() {
        // utility class
    }

    /**
     * Build a KafkaSink that writes JSON value bytes and uses String key for partitioning.
     *
     * Expected cfg keys:
     * - Required: bootstrap.servers, topic
     * - Optional: transaction.timeout.ms, security.protocol, SSL/SASL properties, transactional.id.prefix
     */
    public static KafkaSink<ConsumerGenericRecord> buildJsonKeyValueSink(
            Map<String, Object> cfg,
            Properties extraProducerProps,
            String transactionalIdPrefix,
            DeliveryGuarantee guarantee) {

        Objects.requireNonNull(cfg, "kafka-sink config must not be null");

        String brokers = asString(cfg.get("bootstrap.servers"));
        String topic   = asString(cfg.get("topic"));

        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("kafka-sink.bootstrap.servers is required");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("kafka-sink.topic is required");
        }

        // Collect producer properties
        Properties props = new Properties();
        props.put("acks", "all");

        for (Map.Entry<String, Object> e : cfg.entrySet()) {
            String k = e.getKey();
            if (!"topic".equals(k) && !"bootstrap.servers".equals(k)) {
                props.put(k, String.valueOf(e.getValue()));
            }
        }

        if (extraProducerProps != null) {
            props.putAll(extraProducerProps);
        }

        // Transaction timeout
        props.putIfAbsent("transaction.timeout.ms", "300000"); // 5 min

        // Transactional ID prefix
        String txPrefix = asString(cfg.get("transactional.id.prefix"));
        if (txPrefix == null || txPrefix.isEmpty()) {
            txPrefix = transactionalIdPrefix;
        }

        return KafkaSink.<ConsumerGenericRecord>builder()
                .setBootstrapServers(brokers)
                .setDeliveryGuarantee(guarantee)
                .setTransactionalIdPrefix(txPrefix)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setKeySerializationSchema((ConsumerGenericRecord rec) -> rec.getKey().getBytes(StandardCharsets.UTF_8))
                                .setValueSerializationSchema(new JsonMapValueSerializer())
                                .build()
                )
                .setKafkaProducerConfig(props)
                .build();
    }

    /**
     * Convenience overload with sensible defaults:
     * - EXACTLY_ONCE
     * - transactional.id.prefix = "dedup-to-kafka-json"
     * - acks = all
     */
    public static KafkaSink<ConsumerGenericRecord> buildJsonKeyValueSink(Map<String, Object> cfg) {
        return buildJsonKeyValueSink(
                cfg,
                null,
                "dedup-to-kafka-json",
                DeliveryGuarantee.EXACTLY_ONCE
        );
    }

    public static KafkaSink<ConsumerGenericRecord> buildAvroKeyValueSink(Map<String, Object> cfg) {
        Objects.requireNonNull(cfg, "kafka-sink-avro config must not be null");

        String brokers = asString(cfg.get("bootstrap.servers"));
        String topic   = asString(cfg.get("topic"));
        String subject = asString(cfg.getOrDefault("value.subject", topic == null ? null : topic + "-value"));

        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("bootstrap.servers is required");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (cfg.get("schema.registry.url") == null) {
            throw new IllegalArgumentException("schema.registry.url is required for Avro sink");
        }

        Properties props = new Properties();
        props.put("acks", "all");
        // pass-through extra producer/serializer settings (security, SR, transactions, etc.)
        for (Map.Entry<String, Object> e : cfg.entrySet()) {
            String k = e.getKey();
            if (!"topic".equals(k) && !"bootstrap.servers".equals(k)) {
                props.put(k, String.valueOf(e.getValue()));
            }
        }
        props.putIfAbsent("transaction.timeout.ms", "300000"); // 5m

        return KafkaSink.<ConsumerGenericRecord>builder()
                .setBootstrapServers(brokers)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("dedup-to-kafka-avro")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setKeySerializationSchema((ConsumerGenericRecord rec) ->
                                        rec.getKey() == null ? null : rec.getKey().getBytes(StandardCharsets.UTF_8))
                                .setValueSerializationSchema(new AvroValueSerializer(cfg, topic, subject))
                                .build()
                )
                .setKafkaProducerConfig(props)
                .build();
    }


    private static String asString(Object o) {
        return o == null ? null : String.valueOf(o);
    }

}
