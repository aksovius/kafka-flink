package com.visa.flink.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.visa.flink.model.ConsumerGenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Base64;

/**
 * Custom serializer: converts ConsumerGenericRecord value map into JSON.
 * Handles nested types and normalizes values to be JSON-safe.
 */
public class JsonMapValueSerializer implements SerializationSchema<ConsumerGenericRecord> {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private static final java.nio.charset.Charset UTF8 = StandardCharsets.UTF_8;

    @Override
    public byte[] serialize(ConsumerGenericRecord element) {
        if (element == null || element.getValue() == null) {
            return null;
        }

        Map<String, Object> map = element.getValue();
        Map<String, Object> normalized = normalizeMap(map);

        try {
            return MAPPER.writeValueAsBytes(normalized);
        } catch (JsonProcessingException e) {
            // For production consider DLQ
            throw new RuntimeException("JSON serialization failed for: " + normalized, e);
        }
    }

    // ---------------- Normalization Helpers ----------------

    private static Map<String, Object> normalizeMap(Map<String, Object> in) {
        Map<String, Object> out = new LinkedHashMap<>(in.size());
        for (Map.Entry<String, Object> e : in.entrySet()) {
            out.put(e.getKey(), normalizeValue(e.getValue()));
        }
        return out;
    }

    private static Object normalizeValue(Object v) {
        if (v == null) return null;

        // Primitive wrappers
        if (v instanceof String || v instanceof Number || v instanceof Boolean) {
            return v;
        }

        // Bytes
        if (v instanceof byte[]) {
            return bytesToTextOrBase64((byte[]) v);
        }

        // ByteBuffer
        if (v instanceof ByteBuffer bb) {
            byte[] bytes = new byte[bb.remaining()];
            bb.duplicate().get(bytes);
            return bytesToTextOrBase64(bytes);
        }

        // Kafka Bytes (org.apache.kafka.common.utils.Bytes)
        if (v.getClass().getName().equals("org.apache.kafka.common.utils.Bytes")) {
            try {
                var getMethod = v.getClass().getMethod("get");
                byte[] bytes = (byte[]) getMethod.invoke(v);
                return bytesToTextOrBase64(bytes);
            } catch (ReflectiveOperationException ignored) {
            }
        }

        // List
        if (v instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object o : list) {
                out.add(normalizeValue(o));
            }
            return out;
        }

        // Map
        if (v instanceof Map<?, ?> m) {
            // special case: wrapper that looks like {"bytes": "...", "length":..., "byteLength":..., "empty":...}
            if (looksLikeByteWrapper(m)) {
                Object obj = m.get("bytes");
                if (obj instanceof String s) {
                    try {
                        byte[] decoded = Base64.getDecoder().decode(s);
                        return bytesToTextOrBase64(decoded);
                    } catch (IllegalArgumentException bad64) {
                        return s; // not valid base64, return as-is
                    }
                }
            }
            Map<String, Object> out = new LinkedHashMap<>(m.size());
            for (Map.Entry<?, ?> e : m.entrySet()) {
                Object key = e.getKey();
                if (key != null) {
                    out.put(String.valueOf(key), normalizeValue(e.getValue()));
                }
            }
            return out;
        }

        // Buffer-like with getBytes or toByteArray
        try {
            var getBytesMethod = v.getClass().getMethod("getBytes");
            if (getBytesMethod.getReturnType() == byte[].class) {
                byte[] bytes = (byte[]) getBytesMethod.invoke(v);
                return bytesToTextOrBase64(bytes);
            }
        } catch (ReflectiveOperationException ignored) {
        }

        try {
            var toByteArrayMethod = v.getClass().getMethod("toByteArray");
            if (toByteArrayMethod.getReturnType() == byte[].class) {
                byte[] bytes = (byte[]) toByteArrayMethod.invoke(v);
                return bytesToTextOrBase64(bytes);
            }
        } catch (ReflectiveOperationException ignored) {
        }

        // Fallback
        return v.toString();
    }

    private static boolean looksLikeByteWrapper(Map<?, ?> m) {
        if (!m.containsKey("bytes")) return false;
        for (Object kObj : m.keySet()) {
            String k = String.valueOf(kObj);
            if (!k.equals("bytes") && !k.equals("length") && !k.equals("byteLength") && !k.equals("empty")) {
                return false;
            }
        }
        return true;
    }

    private static String bytesToTextOrBase64(byte[] bytes) {
        // try UTF-8 if mostly printable, else base64
        String asText = new String(bytes, UTF8);
        if (isMostlyPrintable(asText)) return asText;
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static boolean isMostlyPrintable(String s) {
        if (s.isEmpty()) return true;
        int printable = 0;
        int len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c == '\n' || c == '\r' || c == '\t') {
                printable++;
                continue;
            }
            if (c >= 32 && c < 127) {
                printable++;
            }
        }
        // >= 85% printable considered text
        return printable >= (len * 85 / 100);
    }
}
