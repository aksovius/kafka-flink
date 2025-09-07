-- Create database
CREATE DATABASE IF NOT EXISTS hades;

-- Use the database
USE hades;

-- Create deduplication table
CREATE TABLE IF NOT EXISTS auth_dedup (
    id String,
    event_time DateTime64(3),
    processed_time DateTime64(3) DEFAULT now64(3),
    payload String,
    ttl_seconds UInt32 DEFAULT 300
) ENGINE = MergeTree()
ORDER BY (id, event_time)
TTL processed_time + INTERVAL ttl_seconds SECOND
SETTINGS index_granularity = 8192;

-- Create test topics table for monitoring
CREATE TABLE IF NOT EXISTS kafka_topics (
    topic_name String,
    partition_id Int32,
    offset Int64,
    created_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (topic_name, partition_id, created_at)
SETTINGS index_granularity = 8192;
