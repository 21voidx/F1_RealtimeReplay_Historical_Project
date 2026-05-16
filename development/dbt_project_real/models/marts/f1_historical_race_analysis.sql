WITH first_position_record AS (
    SELECT
        session_key,
        driver_number,
        position as first_position,
        date
    FROM {{ ref('stg_f1_position') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_key, driver_number ORDER BY date) = 1
),
lap_metrics AS (
    SELECT
        l.lap_number,
        l.session_key,
        l.driver_number,
        l.date_start,
        l.date_end,
        l.is_pit_out_lap,
        l.duration_sector_1,
        l.duration_sector_2,
        l.duration_sector_3,
        fpr.first_position,
        MIN(i.gap_to_leader) AS min_gap_to_leader,
        MAX(i.gap_to_leader) AS max_gap_to_leader,
        MIN(i.gap_to_next) AS min_gap_to_next,
        MAX(i.gap_to_next) AS max_gap_to_next,
        sp.position AS start_position,
        ep.position AS end_position
    FROM {{ ref('stg_f1_laps') }} l
    LEFT JOIN first_position_record fpr
        ON l.session_key = fpr.session_key
        AND l.driver_number = fpr.driver_number
    LEFT JOIN {{ ref('stg_f1_intervals') }} i
        ON i.session_key = l.session_key
        AND i.driver_number = l.driver_number
        AND DATE_TRUNC('SECOND', i.date) BETWEEN l.date_start AND l.date_end
    LEFT JOIN {{ ref('int_lap_positions') }} sp
        ON l.session_key = sp.session_key
        AND l.driver_number = sp.driver_number
        AND l.lap_number = sp.lap_number
        AND sp.start_position_rank = 1
    LEFT JOIN {{ ref('int_lap_positions') }} ep
        ON l.session_key = ep.session_key
        AND l.driver_number = ep.driver_number
        AND l.lap_number = ep.lap_number
        AND ep.end_position_rank = 1
    GROUP BY 1,2,3,4,5,6,7,8,9,10,15,16
),
filled_positions AS (
    SELECT
        *,
        CASE
            WHEN lap_number = 1 THEN first_position
            ELSE COALESCE(
                start_position,
                FIRST_VALUE(start_position IGNORE NULLS) OVER (
                    PARTITION BY session_key, driver_number
                    ORDER BY lap_number DESC
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                ),
                LAST_VALUE(start_position IGNORE NULLS) OVER (
                    PARTITION BY session_key, driver_number
                    ORDER BY lap_number
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                first_position
            )
        END AS filled_start_position,
        CASE
            WHEN lap_number = 1 THEN first_position
            ELSE COALESCE(
                end_position,
                FIRST_VALUE(end_position IGNORE NULLS) OVER (
                    PARTITION BY session_key, driver_number
                    ORDER BY lap_number DESC
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                ),
                LAST_VALUE(end_position IGNORE NULLS) OVER (
                    PARTITION BY session_key, driver_number
                    ORDER BY lap_number
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                first_position
            )
        END AS filled_end_position
    FROM lap_metrics
)
SELECT * FROM filled_positions