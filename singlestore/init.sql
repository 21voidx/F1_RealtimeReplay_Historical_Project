CREATE DATABASE IF NOT EXISTS f1_realtime;
USE f1_realtime;

CREATE ROWSTORE TABLE IF NOT EXISTS f1_location_data (
  id VARCHAR(255) PRIMARY KEY,
  x DOUBLE NULL,
  y DOUBLE NULL,
  z DOUBLE NULL,
  driver_number INT,
  date DATETIME,
  session_key INT,
  meeting_key INT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE ROWSTORE TABLE IF NOT EXISTS f1_car_data (
  id VARCHAR(255) PRIMARY KEY,
  brake INT,
  drs INT,
  n_gear INT,
  rpm INT,
  speed INT,
  throttle INT,
  driver_number INT,
  date DATETIME,
  session_key INT,
  meeting_key INT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY idx_car_date_driver (date, driver_number),
  KEY idx_car_session (session_key)
);

CREATE ROWSTORE TABLE IF NOT EXISTS f1_intervals_data (
  id VARCHAR(255) PRIMARY KEY,
  driver_number INT,
  gap_to_leader DOUBLE NULL,
  time_interval DOUBLE NULL,
  date DATETIME,
  session_key INT,
  meeting_key INT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE ROWSTORE TABLE IF NOT EXISTS f1_position_data (
  id VARCHAR(255) PRIMARY KEY,
  position INT,
  driver_number INT,
  date DATETIME,
  session_key INT,
  meeting_key INT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP PIPELINE IF EXISTS f1_location_pipeline;
DROP PIPELINE IF EXISTS f1_car_pipeline;
DROP PIPELINE IF EXISTS f1_intervals_pipeline;
DROP PIPELINE IF EXISTS f1_position_pipeline;

CREATE PIPELINE f1_location_pipeline AS
LOAD DATA KAFKA 'kafka:29092/topic_0'
SKIP DUPLICATE KEY ERRORS
INTO TABLE f1_location_data
FORMAT JSON
(
  id <- payload::id,
  x <- payload::x,
  y <- payload::y,
  z <- payload::z,
  driver_number <- payload::driver_number,
  date <- payload::date,
  session_key <- payload::session_key,
  meeting_key <- payload::meeting_key
);

CREATE PIPELINE f1_car_pipeline AS
LOAD DATA KAFKA 'kafka:29092/topic_1'
SKIP DUPLICATE KEY ERRORS
INTO TABLE f1_car_data
FORMAT JSON
(
  id <- payload::id,
  brake <- payload::brake,
  drs <- payload::drs,
  n_gear <- payload::n_gear,
  rpm <- payload::rpm,
  speed <- payload::speed,
  throttle <- payload::throttle,
  driver_number <- payload::driver_number,
  date <- payload::date,
  session_key <- payload::session_key,
  meeting_key <- payload::meeting_key
);

CREATE PIPELINE f1_intervals_pipeline AS
LOAD DATA KAFKA 'kafka:29092/topic_2'
SKIP DUPLICATE KEY ERRORS
INTO TABLE f1_intervals_data
FORMAT JSON
(
  id <- payload::id,
  driver_number <- payload::driver_number,
  gap_to_leader <- payload::gap_to_leader,
  time_interval <- payload::time_interval,
  date <- payload::date,
  session_key <- payload::session_key,
  meeting_key <- payload::meeting_key
);

CREATE PIPELINE f1_position_pipeline AS
LOAD DATA KAFKA 'kafka:29092/topic_3'
SKIP DUPLICATE KEY ERRORS
INTO TABLE f1_position_data
FORMAT JSON
(
  id <- payload::id,
  position <- payload::position,
  driver_number <- payload::driver_number,
  date <- payload::date,
  session_key <- payload::session_key,
  meeting_key <- payload::meeting_key
);

START PIPELINE f1_location_pipeline;
START PIPELINE f1_car_pipeline;
START PIPELINE f1_intervals_pipeline;
START PIPELINE f1_position_pipeline;
