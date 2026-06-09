CREATE DATABASE IF NOT EXISTS metrics;

CREATE TABLE IF NOT EXISTS metrics.dashboard (
    server_id String,
    cpu_usage Float64,
    memory_usage Float64,
    timestamp DateTime
)
ENGINE = MergeTree
ORDER BY (server_id, timestamp);

CREATE TABLE IF NOT EXISTS metrics.alerts (
    server_id String,
    alert_type String,
    metric_value Float64,
    event_timestamp DateTime,
    detected_at DateTime
)
ENGINE = MergeTree
ORDER BY (server_id, detected_at);
