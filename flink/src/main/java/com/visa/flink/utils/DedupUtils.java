package com.visa.flink.utils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Properties;

public class DedupUtils {

    public static Properties loadPropertiesFromResource(String resourceName) {
        Properties props = new Properties();
        try (InputStream input = DedupUtils.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (input == null) {
                throw new IOException("Resource not found: " + resourceName);
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Could not load properties from resource: " + resourceName, e);
        }
        return props;
    }

    public static String getInsertSql(String database, String tableName, Field[] fields) {
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();

        for (Field f : fields) {
            if (columns.length() > 0) {
                columns.append(", ");
                placeholders.append(", ");
            }
            columns.append(f.getName());
            placeholders.append("?");
        }

        return String.format("INSERT INTO %s.%s (%s) VALUES (%s)", database, tableName, columns, placeholders);
    }
}
