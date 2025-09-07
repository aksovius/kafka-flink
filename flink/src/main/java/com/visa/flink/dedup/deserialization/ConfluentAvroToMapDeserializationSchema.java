package com.visa.flink.dedup.deserialization;

import com.visa.flink.model.ConsumerGenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializer for Confluent Avro (Schema Registry).
 * Converts Avro GenericRecord into Map<String,Object>
 * and wraps into ConsumerGenericRecord.
 */
public class ConfluentAvroToMapDeserializationSchema
        implements KafkaRecordDeserializationSchema<ConsumerGenericRecord> {

    private final Map<String, Object> config;
    private transient KafkaAvroDeserializer avroDeserializer;

    public ConfluentAvroToMapDeserializationSchema(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<ConsumerGenericRecord> out) {

        // lazy init because class must be serializable
        if (avroDeserializer == null) {
            avroDeserializer = new KafkaAvroDeserializer();
            avroDeserializer.configure(config, /* isKey= */ false);
        }

        // Key
        String key = record.key() == null
                ? null
                : new String(record.key(), StandardCharsets.UTF_8);

        // Value (Avro -> Map)
        GenericRecord genericRecord = (GenericRecord) avroDeserializer.deserialize(
                record.topic(), record.value());

        Map<String, Object> map = new HashMap<>();
        if (genericRecord != null) {
            Schema schema = genericRecord.getSchema();
            for (Schema.Field f : schema.getFields()) {
                map.put(f.name(), genericRecord.get(f.name()));
            }
        }

        out.collect(new ConsumerGenericRecord(key, map));
    }

    @Override
    public TypeInformation<ConsumerGenericRecord> getProducedType() {
        return TypeInformation.of(new TypeHint<>() {});
    }
}
