SELECT
    session_key,
    driver_number,
    MIN(lap_duration) AS best_lap_time
FROM {{ ref('stg_f1_laps') }}
WHERE lap_duration > 0
GROUP BY session_key, driver_number