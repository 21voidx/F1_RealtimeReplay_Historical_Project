CREATE DATABASE IF NOT EXISTS openf1_replay;

CREATE TABLE IF NOT EXISTS openf1_replay.car_data
(
    replay_id String,
    event_time DateTime64(3, 'UTC'),
    emitted_at DateTime64(3, 'UTC'),
    meeting_key UInt32,
    session_key UInt32,
    driver_number UInt16,
    brake UInt8,
    drs UInt8,
    n_gear UInt8,
    rpm UInt32,
    speed UInt16,
    throttle UInt8
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (replay_id, session_key, driver_number, event_time);

CREATE TABLE IF NOT EXISTS openf1_replay.position
(
    replay_id String,
    event_time DateTime64(3, 'UTC'),
    emitted_at DateTime64(3, 'UTC'),
    meeting_key UInt32,
    session_key UInt32,
    driver_number UInt16,
    position UInt16
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (replay_id, session_key, driver_number, event_time);

CREATE TABLE IF NOT EXISTS openf1_replay.location
(
    replay_id String,
    event_time DateTime64(3, 'UTC'),
    emitted_at DateTime64(3, 'UTC'),
    meeting_key UInt32,
    session_key UInt32,
    driver_number UInt16,
    x Int32,
    y Int32,
    z Int32
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (replay_id, session_key, driver_number, event_time);

CREATE TABLE IF NOT EXISTS openf1_replay.car_data_queue
(
    replay_id String,
    event_time String,
    emitted_at String,
    meeting_key UInt32,
    session_key UInt32,
    driver_number UInt16,
    brake UInt8,
    drs UInt8,
    n_gear UInt8,
    rpm UInt32,
    speed UInt16,
    throttle UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'openf1.car_data',
    kafka_group_name = 'clickhouse-openf1-car-data',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 100,
    kafka_thread_per_consumer = 1;

CREATE TABLE IF NOT EXISTS openf1_replay.position_queue
(
    replay_id String,
    event_time String,
    emitted_at String,
    meeting_key UInt32,
    session_key UInt32,
    driver_number UInt16,
    position UInt16
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'openf1.position',
    kafka_group_name = 'clickhouse-openf1-position',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 100,
    kafka_thread_per_consumer = 1;

CREATE TABLE IF NOT EXISTS openf1_replay.location_queue
(
    replay_id String,
    event_time String,
    emitted_at String,
    meeting_key UInt32,
    session_key UInt32,
    driver_number UInt16,
    x Int32,
    y Int32,
    z Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'openf1.location',
    kafka_group_name = 'clickhouse-openf1-location',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 100,
    kafka_thread_per_consumer = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS openf1_replay.car_data_mv
TO openf1_replay.car_data
AS
SELECT
    replay_id,
    parseDateTime64BestEffortOrZero(event_time, 3, 'UTC') AS event_time,
    parseDateTime64BestEffortOrZero(emitted_at, 3, 'UTC') AS emitted_at,
    meeting_key,
    session_key,
    driver_number,
    brake,
    drs,
    n_gear,
    rpm,
    speed,
    throttle
FROM openf1_replay.car_data_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS openf1_replay.position_mv
TO openf1_replay.position
AS
SELECT
    replay_id,
    parseDateTime64BestEffortOrZero(event_time, 3, 'UTC') AS event_time,
    parseDateTime64BestEffortOrZero(emitted_at, 3, 'UTC') AS emitted_at,
    meeting_key,
    session_key,
    driver_number,
    position
FROM openf1_replay.position_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS openf1_replay.location_mv
TO openf1_replay.location
AS
SELECT
    replay_id,
    parseDateTime64BestEffortOrZero(event_time, 3, 'UTC') AS event_time,
    parseDateTime64BestEffortOrZero(emitted_at, 3, 'UTC') AS emitted_at,
    meeting_key,
    session_key,
    driver_number,
    x,
    y,
    z
FROM openf1_replay.location_queue;
