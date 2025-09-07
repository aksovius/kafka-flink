package com.visa.flink.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.visa.flink.model.ConsumerGenericRecord;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonDeserializationSchema implements KafkaRecordDeserializationSchema<ConsumerGenericRecord> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, org.apache.flink.util.Collector<ConsumerGenericRecord> out) throws IOException {
        if (record.value() == null) {
            return;
        }
        
        String jsonString = new String(record.value(), StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        Map<String, Object> valueMap = objectMapper.readValue(jsonString, Map.class);
        
        // Use Kafka key if available, otherwise extract from value
        String key;
        if (record.key() != null) {
            key = new String(record.key(), StandardCharsets.UTF_8);
        } else {
            key = (String) valueMap.get("authId");
            if (key == null) {
                key = "unknown";
            }
        }
        
        out.collect(new ConsumerGenericRecord(key, valueMap));
    }
    
    @Override
    public TypeInformation<ConsumerGenericRecord> getProducedType() {
        return TypeInformation.of(ConsumerGenericRecord.class);
    }
}
