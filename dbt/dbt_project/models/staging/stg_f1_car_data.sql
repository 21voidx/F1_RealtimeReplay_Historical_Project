SELECT
    session_key,
    driver_number::VARCHAR(50) as driver_number,
    TO_TIMESTAMP_NTZ(date) as timestamp,
    rpm,
    speed,
    throttle,
    brake,
    drs
FROM {{ source('F1_API', 'RAW_CAR_DATA') }}