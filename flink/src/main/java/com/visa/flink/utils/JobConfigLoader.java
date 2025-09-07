package com.visa.flink.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.Map;

public class JobConfigLoader {
    private static Map<String, Object> config;

    public static Map<String, Object> loadConfig(String filePath) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            config = mapper.readValue(new File(filePath), new TypeReference<>() {});
            return config;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load job.json from path: " + filePath, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getSection(String key) {
        if (config == null) throw new IllegalStateException("Config not loaded. Call loadConfig() first.");
        Map<String, Object> section = (Map<String, Object>) config.get(key);
        if (section != null) return section;
        throw new IllegalArgumentException("Section not found: " + key);
    }

    public static String getProperty(String section, String property) {
        Map<String, Object> sec = getSection(section);
        Object value = sec.get(property);
        return value == null ? null : value.toString();
    }
}
