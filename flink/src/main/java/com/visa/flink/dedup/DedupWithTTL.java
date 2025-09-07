package com.visa.flink.dedup;

import com.visa.flink.model.ConsumerGenericRecord;
import com.visa.flink.utils.AppLogger;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Emits the first record for each key immediately, then ignores further records
 * for that key until TTL expires.
 */
public class DedupWithTTL
        extends KeyedProcessFunction<String, ConsumerGenericRecord, ConsumerGenericRecord> {

    private final long ttlMillis;

    // Tracks if we've seen this key within TTL
    private transient ValueState<Boolean> seenState;

    // Tracks when TTL expires for this key
    private transient ValueState<Long> expiryState;

    public DedupWithTTL(Long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    @Override
    public void open(Configuration parameters) {
        seenState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "seenState",
                        TypeInformation.of(new TypeHint<Boolean>() {})
                ));

        expiryState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "expiryState",
                        TypeInformation.of(new TypeHint<Long>() {})
                ));

        AppLogger.info("msg", "DeduplicateFirstWithTTL initialized with TTL={} ms", ttlMillis);
    }

    @Override
    public void processElement(
            ConsumerGenericRecord value,
            Context ctx,
            Collector<ConsumerGenericRecord> out) throws Exception {

        Boolean seen = seenState.value();
        Long expiry = expiryState.value();
        long now = ctx.timerService().currentProcessingTime();

        if (seen == null || !seen || (expiry != null && now >= expiry)) {
            // First occurrence or TTL expired → emit
            out.collect(value);

            seenState.update(true);
            long expireAt = now + ttlMillis;
            expiryState.update(expireAt);
            ctx.timerService().registerProcessingTimeTimer(expireAt);

            AppLogger.info("msg", "First occurrence for key {} emitted, valid until {}",
                    ctx.getCurrentKey(), expiry);
        } else {
            // Duplicate within TTL → ignore
            AppLogger.info("msg", "Duplicate within TTL for key {} ignored",
                    ctx.getCurrentKey());
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<ConsumerGenericRecord> out) {
        // TTL expired → allow key to be emitted again
        try {
            seenState.clear();
            expiryState.clear();
            AppLogger.debug("msg", "TTL expired for key {}, state cleared",
                    ctx.getCurrentKey());
        } catch (Exception e) {
            throw new RuntimeException("Error clearing state for key " + ctx.getCurrentKey(), e);
        }
    }
}
