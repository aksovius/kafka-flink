package com.visa.flink.serialization;

import com.visa.flink.model.ConsumerGenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Serialize ConsumerGenericRecord.value (Map<String, Object>) into Avro bytes
 * using Confluent KafkaAvroSerializer + Schema Registry.
 *
 * Expected cfg keys (value side):
 *  - bootstrap.servers          (handled by KafkaSinkFactory)
 *  - topic                      (used to derive default subject "<topic>-value")
 *  - schema.registry.url        (http(s)://host:8081)
 *  - value.subject              (optional; default "<topic>-value")
 *  - value.avro.schema          (optional schema string; if missing -> fetch latest from registry)
 *  - auto.register.schemas      (optional; "false" by default)
 *  - value.subject.name.strategy (optional; passed through if needed)
 */
public class AvroValueSerializer implements SerializationSchema<ConsumerGenericRecord> {

    private final Map<String, Object> cfg;
    private final String topic;

    // Lazy / runtime-only
    private transient KafkaAvroSerializer serializer;
    private transient Schema schema;

    public AvroValueSerializer(Map<String, Object> cfg, String topic, String subjectOverride) {
        this.cfg = cfg;
        this.topic = topic;
    }

    @Override
    public byte[] serialize(ConsumerGenericRecord element) {
        ensureInit();

        if (element == null || element.getValue() == null) {
            return null;
        }

        Map<String, Object> map = element.getValue();
        GenericRecord record = mapToRecord(map, schema);
        return serializer.serialize(topic, record);
    }

    // ---------------- init & helpers ----------------

    private void ensureInit() {
        if (serializer == null) {
            serializer = new KafkaAvroSerializer();
            // Pass through all Confluent props (schema.registry.url, subject strategies, etc.)
            Map<String, Object> props = new HashMap<>();
            for (Map.Entry<String, Object> e : cfg.entrySet()) {
                props.put(e.getKey(), String.valueOf(e.getValue()));
            }
            // safer default: do NOT auto-register unless explicitly requested
            props.putIfAbsent("auto.register.schemas", "false");
            serializer.configure(props, /*isKey=*/false);
        }

        if (schema == null) {
            // Try inline schema in cfg
            String schemaStr = asString(cfg.get("value.avro.schema"));
            if (schemaStr == null) {
                throw new IllegalStateException(
                    "Schema not provided. Provide cfg.value.avro.schema."
                );
            }
            schema = new Schema.Parser().parse(schemaStr);
        }
    }

    private static String asString(Object o) {
        return (o == null) ? null : String.valueOf(o);
    }

    private static GenericRecord mapToRecord(Map<String, Object> map, Schema schema) {
        GenericData.Record rec = new GenericData.Record(schema);
        for (Schema.Field f : schema.getFields()) {
            Object v = map.get(f.name());
            rec.put(f.name(), toAvro(v, f.schema()));
        }
        return rec;
    }

    @SuppressWarnings("unchecked")
    private static Object toAvro(Object v, Schema s) {
        if (v == null) {
            // handle unions with null
            if (s.getType() == Type.UNION) {
                for (Schema branch : s.getTypes()) {
                    if (branch.getType() == Type.NULL) return null;
                }
            }
            // plain null
            return null;
        }

        switch (s.getType()) {
            case STRING:
                return v instanceof CharSequence ? v : String.valueOf(v);

            case INT:
                if (v instanceof Number) return ((Number) v).intValue();
                if (v instanceof String) return Integer.parseInt((String) v);
                break;

            case LONG:
                if (v instanceof Number) return ((Number) v).longValue();
                if (v instanceof String) return Long.parseLong((String) v);
                break;

            case FLOAT:
                if (v instanceof Number) return ((Number) v).floatValue();
                if (v instanceof String) return Float.parseFloat((String) v);
                break;

            case DOUBLE:
                if (v instanceof Number) return ((Number) v).doubleValue();
                if (v instanceof String) return Double.parseDouble((String) v);
                break;

            case BOOLEAN:
                if (v instanceof Boolean) return v;
                if (v instanceof String) return Boolean.parseBoolean((String) v);
                if (v instanceof Number) return ((Number) v).intValue() != 0;
                break;

            case BYTES:
                if (v instanceof byte[] b) return ByteBuffer.wrap(b);
                if (v instanceof ByteBuffer bb) return bb;
                if (v instanceof String sVal) return ByteBuffer.wrap(sVal.getBytes(StandardCharsets.UTF_8));
                break;

            case ENUM:
                return new GenericData.EnumSymbol(s, String.valueOf(v));

            case FIXED:
                if (v instanceof byte[] b2) return new GenericData.Fixed(s, b2);
                break;

            case ARRAY:
                if (v instanceof Collection<?> col) {
                    List<Object> out = new ArrayList<>(col.size());
                    Schema elem = s.getElementType();
                    for (Object o : col) out.add(toAvro(o, elem));
                    return out;
                }
                break;

            case MAP:
                if (v instanceof Map<?, ?> m) {
                    Map<String, Object> out = new HashMap<>();
                    Schema valSchema = s.getValueType();
                    for (Map.Entry<?, ?> e : m.entrySet()) {
                        String key = String.valueOf(e.getKey()); // Avro map keys are strings
                        out.put(key, toAvro(e.getValue(), valSchema));
                    }
                    return out;
                }
                break;

            case RECORD:
                if (v instanceof Map<?, ?> mrec) {
                    return mapToRecord((Map<String, Object>) mrec, s);
                }
                break;

            case UNION:
                // try non-null branch first
                for (Schema branch : s.getTypes()) {
                    if (branch.getType() == Type.NULL) continue;
                    try {
                        Object converted = toAvro(v, branch);
                        // ensure compatibility (GenericData validation)
                        if (converted == null && branch.getType() != Type.NULL) continue;
                        return converted;
                    } catch (Exception ignore) { /* try next */ }
                }
                // fall back to null branch if exists
                for (Schema branch : s.getTypes()) {
                    if (branch.getType() == Type.NULL) return null;
                }
                break;

            case NULL:
                return null;
        }

        // Fallback: stringify
        return String.valueOf(v);
    }
}
