WITH source AS (
    SELECT * FROM {{ source('HITESH', 'F1_LAPS') }}
)
SELECT
    session_key,
    driver_number,
    DATE_TRUNC('second', date_start) AS date_start,
    DATEADD(second, lap_duration, DATE_TRUNC('second', date_start)) AS date_end,
    lap_duration,
    lap_number,
    is_pit_out_lap,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3
FROM source