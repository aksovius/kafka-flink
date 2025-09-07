package com.visa.flink.model;

import java.util.Map;
import java.util.Objects;

/**
 * Payload model class для хранения данных события
 */
public class Payload {
    private final Map<String, Object> data;
    private final long timestamp;
    private final String eventType;

    public Payload(Map<String, Object> data, long timestamp, String eventType) {
        this.data = data;
        this.timestamp = timestamp;
        this.eventType = eventType;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getEventType() {
        return eventType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Payload payload = (Payload) o;
        return timestamp == payload.timestamp &&
                Objects.equals(data, payload.data) &&
                Objects.equals(eventType, payload.eventType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, timestamp, eventType);
    }

    @Override
    public String toString() {
        return "Payload{" +
                "data=" + data +
                ", timestamp=" + timestamp +
                ", eventType='" + eventType + '\'' +
                '}';
    }
}
