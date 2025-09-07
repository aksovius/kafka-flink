package com.visa.flink.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import com.visa.flink.model.ConsumerGenericRecord;
import com.visa.flink.sink.KafkaSinkFactory;
import com.visa.flink.dedup.DedupWithTTL;
import com.visa.flink.dedup.deserialization.ConfluentAvroToMapDeserializationSchema;
import com.visa.flink.utils.AppLogger;
import com.visa.flink.utils.JobConfigLoader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Map;

public class FlinkConsumerUnionDedupTTLJob {

    public static void main(String[] args) {
        try {
            // Load job config
            String jobJsonPath = args.length > 0 ? args[0] : "/data/hades-dedup/job.json";
            AppLogger.info("msg", "json path is: {}, file path is: {}", jobJsonPath, jobJsonPath);
            Map<String, Object> loadedConfig = JobConfigLoader.loadConfig(jobJsonPath);

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
            KafkaSource<ConsumerGenericRecord> sourceVDS_QA =
                    createKafkaSourceFromConfig((Map<String, Object>) loadedConfig.get("qa"));
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
            Map<String, Object> outKafkaJsonCfg = (Map<String, Object>) loadedConfig.get("kafka-sink-json");
            KafkaSink<ConsumerGenericRecord> kafkaJsonSink =
                    KafkaSinkFactory.buildJsonKeyValueSink(outKafkaJsonCfg);

            deduped
                    .map(rec -> new ConsumerGenericRecord(rec.getKey(), rec.getValue()))
                    .sinkTo(kafkaJsonSink)
                    .name("Kafka-Sink-JSON")
                    .uid("kafka-sink-json");

            // Avro sink
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
        String securityProtocol = (String) config.get("security.protocol");
        String schemaRegistryUrl = (String) config.get("schema.registry.url");
        
        if (bootstrapServers == null || topic == null) {
            throw new IllegalArgumentException("bootstrap.servers and topic are required in config");
        }

        // Создаем конфигурацию для Confluent Avro десериализатора
        Map<String, Object> deserializerConfig = new java.util.HashMap<>();
        deserializerConfig.put("bootstrap.servers", bootstrapServers);
        deserializerConfig.put("group.id", groupId);
        deserializerConfig.put("security.protocol", securityProtocol);
        deserializerConfig.put("ssl.truststore.location", config.get("ssl.truststore.location"));
        deserializerConfig.put("ssl.truststore.password", config.get("ssl.truststore.password"));
        deserializerConfig.put("ssl.keystore.location", config.get("ssl.keystore.location"));
        deserializerConfig.put("ssl.keystore.password", config.get("ssl.keystore.password"));
        deserializerConfig.put("schema.registry.url", schemaRegistryUrl);
        deserializerConfig.put("specific.avro.reader", config.get("specific.avro.reader"));

        return KafkaSource.<ConsumerGenericRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new ConfluentAvroToMapDeserializationSchema(deserializerConfig))
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
}
