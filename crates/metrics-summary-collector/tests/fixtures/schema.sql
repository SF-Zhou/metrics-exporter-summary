-- Explicit provisioning example for metric summary storage.
-- The writer only verifies required columns and never executes this file itself.
CREATE DATABASE IF NOT EXISTS metrics_summary;

CREATE TABLE IF NOT EXISTS metrics_summary.distributions
(
    `TIMESTAMP` DateTime,
    `metricName` LowCardinality(String),
    `host` LowCardinality(String),
    `tag` LowCardinality(String),
    `count` Float64,
    `mean` Float64,
    `min` Float64,
    `max` Float64,
    `p50` Float64,
    `p90` Float64,
    `p95` Float64,
    `p99` Float64,
    `mount_name` LowCardinality(String),
    `instance` String,
    `io` LowCardinality(String),
    `uid` LowCardinality(String),
    `method` LowCardinality(String),
    `pod` String,
    `thread` LowCardinality(String),
    `statusCode` LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(TIMESTAMP)
PRIMARY KEY (metricName, host, pod, instance, TIMESTAMP)
ORDER BY (metricName, host, pod, instance, TIMESTAMP)
TTL TIMESTAMP + toIntervalMonth(3)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS metrics_summary.counters
(
    `TIMESTAMP` DateTime,
    `metricName` LowCardinality(String),
    `host` LowCardinality(String),
    `tag` LowCardinality(String),
    `val` Int64,
    `mount_name` LowCardinality(String),
    `instance` String,
    `io` LowCardinality(String),
    `uid` LowCardinality(String),
    `type` LowCardinality(String),
    `pod` String,
    `thread` LowCardinality(String),
    `statusCode` LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(TIMESTAMP)
PRIMARY KEY (metricName, host, pod, instance, TIMESTAMP)
ORDER BY (metricName, host, pod, instance, TIMESTAMP)
TTL TIMESTAMP + toIntervalMonth(3)
SETTINGS index_granularity = 8192;
