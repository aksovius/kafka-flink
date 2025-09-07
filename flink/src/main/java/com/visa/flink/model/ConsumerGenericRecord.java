package com.visa.flink.model;

import java.util.Map;
import java.util.Objects;

/**
 * Consumer generic record model для хранения данных из Kafka
 */
public class ConsumerGenericRecord {
    private final String key;
    private final Map<String, Object> value;

    public ConsumerGenericRecord(String key, Map<String, Object> value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Map<String, Object> getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerGenericRecord that = (ConsumerGenericRecord) o;
        return Objects.equals(key, that.key) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "ConsumerGenericRecord{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
