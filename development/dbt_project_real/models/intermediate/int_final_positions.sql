
WITH latest_positions AS (
    SELECT 
        session_key,
        driver_number,
        position,
        date as latest_date
    FROM {{ ref('stg_f1_position') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY session_key, driver_number 
        ORDER BY date DESC
    ) = 1
)
SELECT * FROM latest_positions