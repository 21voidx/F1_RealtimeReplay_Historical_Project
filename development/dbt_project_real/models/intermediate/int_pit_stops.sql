SELECT
    session_key,
    driver_number,
    COUNT(*) AS pit_stop_count
FROM {{ ref('stg_f1_pit') }}
GROUP BY session_key, driver_number