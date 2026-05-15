WITH positions AS (
    SELECT
        p.session_key,
        p.driver_number,
        p.position,
        p.date,
        l.lap_number,
        l.date_start,
        l.date_end,
        ABS(DATEDIFF(second, p.date, l.date_start)) AS start_time_diff,
        ABS(DATEDIFF(second, p.date, l.date_end)) AS end_time_diff
    FROM {{ ref('stg_f1_position') }} p
    JOIN {{ ref('stg_f1_laps') }} l
        ON p.session_key = l.session_key
        AND p.driver_number = l.driver_number
    WHERE p.date BETWEEN l.date_start AND l.date_end
)
SELECT *,
    ROW_NUMBER() OVER (PARTITION BY session_key, driver_number, lap_number
                       ORDER BY start_time_diff) AS start_position_rank,
    ROW_NUMBER() OVER (PARTITION BY session_key, driver_number, lap_number
                       ORDER BY end_time_diff) AS end_position_rank
FROM positions