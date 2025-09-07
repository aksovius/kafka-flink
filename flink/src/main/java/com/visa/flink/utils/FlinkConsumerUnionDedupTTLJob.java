package com.visa.flink.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import com.visa.flink.model.ConsumerGenericRecord;
import com.visa.flink.sink.KafkaSinkFactory;
import com.visa.flink.dedup.DedupWithTTL;
import com.visa.flink.serialization.JsonDeserializationSchema;
import com.visa.flink.utils.AppLogger;
import com.visa.flink.utils.JobConfigLoader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Map;

public class FlinkConsumerUnionDedupTTLJob {

    public static void main(String[] args) {
        try {
            // Load job config
            Map<String, Object> loadedConfig;
            if (args.length > 0) {
                String jobJsonPath = args[0];
                AppLogger.info("msg", "json path is: {}, file path is: {}", jobJsonPath, jobJsonPath);
                loadedConfig = JobConfigLoader.loadConfig(jobJsonPath);
            } else {
                AppLogger.info("msg", "Using default configuration");
                loadedConfig = createDefaultConfig();
            }

            AppLogger.info("msg", "Starting FlinkConsumerUnionDedupTTLJob...");

            // Create Flink environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            AppLogger.info("msg", "Flink stream execution environment initialized.");

            // -------- Set global parallelism --------
            int globalParallelism = calculateOptimalParallelism();
            env.setParallelism(globalParallelism);
            AppLogger.info("msg", "Set global parallelism to: {}", globalParallelism);

            // Enable checkpointing
            env.enableCheckpointing(60000); // 1 min
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);

            // -------- Create Kafka sources --------
            @SuppressWarnings("unchecked")
            KafkaSource<ConsumerGenericRecord> sourceVDS_QA =
                    createKafkaSourceFromConfig((Map<String, Object>) loadedConfig.get("qa"));
            @SuppressWarnings("unchecked")
            KafkaSource<ConsumerGenericRecord> sourceVDS_DEV =
                    createKafkaSourceFromConfig((Map<String, Object>) loadedConfig.get("dev"));

            DataStream<ConsumerGenericRecord> streamVDSQA =
                    createKafkaDataStream(env, sourceVDS_QA, "KafkaSource-VDSQA", "vds-qa-stream");
            DataStream<ConsumerGenericRecord> streamVDSDEV =
                    createKafkaDataStream(env, sourceVDS_DEV, "KafkaSource-VDSDEV", "vds-dev-stream");

            AppLogger.info("msg", "DataStreams created successfully.");

            // -------- Union two streams --------
            DataStream<ConsumerGenericRecord> unionStream = streamVDSQA.union(streamVDSDEV);
            AppLogger.info("msg", "Union of the two streams...");

            // -------- Deduplicate with TTL --------
            @SuppressWarnings("unchecked")
            long ttlMillis = Long.parseLong(((Map<String, Object>) loadedConfig.get("dedup"))
                    .get("ttl.ms").toString());

            AppLogger.info("msg", "Applying deduplication with TTL ({} ms)...", ttlMillis);

            SingleOutputStreamOperator<ConsumerGenericRecord> deduped =
                    unionStream
                            .keyBy(record -> stripPrefix(record.getKey()))
                            .process(new DedupWithTTL(ttlMillis))
                            .name("deduplicate-latest-with-ttl")
                            .uid("deduplicate-keep-latest-with-ttl")
                            .disableChaining();

            // -------- Write to Kafka sinks --------
            // JSON sink
            @SuppressWarnings("unchecked")
            Map<String, Object> outKafkaJsonCfg = (Map<String, Object>) loadedConfig.get("kafka-sink-json");
            KafkaSink<ConsumerGenericRecord> kafkaJsonSink =
                    KafkaSinkFactory.buildJsonKeyValueSink(outKafkaJsonCfg);

            deduped
                    .map(rec -> new ConsumerGenericRecord(rec.getKey(), rec.getValue()))
                    .sinkTo(kafkaJsonSink)
                    .name("Kafka-Sink-JSON")
                    .uid("kafka-sink-json");

            // Avro sink
            @SuppressWarnings("unchecked")
            Map<String, Object> outKafkaAvroCfg = (Map<String, Object>) loadedConfig.get("kafka-sink-avro");
            KafkaSink<ConsumerGenericRecord> kafkaAvroSink =
                    KafkaSinkFactory.buildAvroKeyValueSink(outKafkaAvroCfg);

            deduped
                    .map(rec -> new ConsumerGenericRecord(rec.getKey(), rec.getValue()))
                    .sinkTo(kafkaAvroSink)
                    .name("Kafka-Sink-AVRO")
                    .uid("kafka-sink-avro");

            // -------- Execute Job --------
            AppLogger.info("msg", "Executing FlinkConsumerUnionDedupTTLJob...");
            env.execute("FlinkConsumerUnionDedupTTLJob");
            AppLogger.info("msg", "FlinkConsumerUnionDedupTTLJob execution submitted.");

        } catch (Exception e) {
            AppLogger.error("msg", "Error in FlinkConsumerUnionDedupTTLJob: ", e);
            throw new RuntimeException("Flink job failed", e);
        }
    }

    // ---------- Utility methods ----------
    private static int calculateOptimalParallelism() {
        // TODO: implement logic
        return 4;
    }

    private static KafkaSource<ConsumerGenericRecord> createKafkaSourceFromConfig(Map<String, Object> config) {
        String bootstrapServers = (String) config.get("bootstrap.servers");
        String topic = (String) config.get("topic");
        String groupId = (String) config.get("group.id");
        
        if (bootstrapServers == null || topic == null) {
            throw new IllegalArgumentException("bootstrap.servers and topic are required in config");
        }

        return KafkaSource.<ConsumerGenericRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new JsonDeserializationSchema())
                .build();
    }

    private static DataStream<ConsumerGenericRecord> createKafkaDataStream(
            StreamExecutionEnvironment env,
            KafkaSource<ConsumerGenericRecord> source,
            String name,
            String uid) {
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), name).uid(uid);
    }

    private static String stripPrefix(String key) {
        // TODO: implement prefix stripping logic if needed
        return key;
    }

    private static Map<String, Object> createDefaultConfig() {
        Map<String, Object> config = new java.util.HashMap<>();
        
        // QA config
        Map<String, Object> qaConfig = new java.util.HashMap<>();
        qaConfig.put("bootstrap.servers", "kafka:29092");
        qaConfig.put("topic", "test-topic-qa");
        qaConfig.put("group.id", "flink-local-consumer-qa");
        qaConfig.put("security.protocol", "PLAINTEXT");
        config.put("qa", qaConfig);
        
        // Dev config
        Map<String, Object> devConfig = new java.util.HashMap<>();
        devConfig.put("bootstrap.servers", "kafka:29092");
        devConfig.put("topic", "test-topic-dev");
        devConfig.put("group.id", "flink-local-consumer-dev");
        devConfig.put("security.protocol", "PLAINTEXT");
        config.put("dev", devConfig);
        
        // JSON sink config
        Map<String, Object> jsonSinkConfig = new java.util.HashMap<>();
        jsonSinkConfig.put("bootstrap.servers", "kafka:29092");
        jsonSinkConfig.put("topic", "prepared_data_json");
        jsonSinkConfig.put("security.protocol", "PLAINTEXT");
        jsonSinkConfig.put("transaction.timeout.ms", "300000");
        config.put("kafka-sink-json", jsonSinkConfig);
        
        // Avro sink config
        Map<String, Object> avroSinkConfig = new java.util.HashMap<>();
        avroSinkConfig.put("bootstrap.servers", "kafka:29092");
        avroSinkConfig.put("topic", "prepared_data_avro");
        avroSinkConfig.put("security.protocol", "PLAINTEXT");
        avroSinkConfig.put("schema.registry.url", "http://schema-registry:8081");
        avroSinkConfig.put("value.subject", "prepared_data_avro-value");
        avroSinkConfig.put("auto.register.schemas", "false");
        avroSinkConfig.put("transaction.timeout.ms", "300000");
        avroSinkConfig.put("value.avro.schema", "{\"type\":\"record\",\"name\":\"AuthEnrich\",\"namespace\":\"com.example.payments\",\"fields\":[{\"name\":\"authId\",\"type\":\"string\"},{\"name\":\"dc\",\"type\":\"string\"},{\"name\":\"eventTime\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"},{\"name\":\"merchantId\",\"type\":\"string\"},{\"name\":\"country\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"riskScore\",\"type\":\"int\"},{\"name\":\"cardBin\",\"type\":\"string\"},{\"name\":\"deviceType\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
        config.put("kafka-sink-avro", avroSinkConfig);
        
        // ClickHouse config (temporarily disabled)
        Map<String, Object> clickhouseConfig = new java.util.HashMap<>();
        clickhouseConfig.put("clickhouse.host", "jdbc:clickhouse://localhost:8123/hades");
        clickhouseConfig.put("clickhouse.table", "auth_dedup");
        clickhouseConfig.put("clickhouse.database", "hades");
        clickhouseConfig.put("clickhouse.pool.size", "10");
        clickhouseConfig.put("clickhouse.connection.timeout", "10000");
        clickhouseConfig.put("clickhouse.socket.timeout", "30000");
        config.put("clickhouse", clickhouseConfig);
        
        // Dedup config
        Map<String, Object> dedupConfig = new java.util.HashMap<>();
        dedupConfig.put("ttl.ms", "300000"); // 5 minutes
        config.put("dedup", dedupConfig);
        
        return config;
    }
}
