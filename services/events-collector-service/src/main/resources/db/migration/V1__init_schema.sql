CREATE DATABASE IF NOT EXISTS iot_analytics;

CREATE TABLE IF NOT EXISTS iot_analytics.device_events
(
    device_id    String,
    event_id     String,
    event_date   Date,
    timestamp_ms Int64,
    type         String,
    payload      String
) ENGINE = MergeTree
    PARTITION BY event_date
    ORDER BY (device_id, event_date, timestamp_ms, event_id);

CREATE TABLE IF NOT EXISTS iot_analytics.device_outbox
(
    device_id  String,
    created_at DateTime,
    status     UInt8,
    sent_at    DateTime,
    attempts   UInt32,
    last_error String
) ENGINE = MergeTree
    PARTITION BY toYYYYMM(created_at)
    ORDER BY (status, created_at, device_id);