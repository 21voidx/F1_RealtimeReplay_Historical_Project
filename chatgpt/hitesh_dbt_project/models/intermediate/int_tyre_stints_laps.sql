SELECT
        s.session_key,
        s.driver_number,
        s.stint_number,
        s.compound,
        s.lap_start,
        s.lap_end,
        s.lap_end - s.lap_start + 1 as stint_length,
        -- Get the start time from the first lap of the stint
        MIN(l.date_start) as stint_start_time,
        -- Get the end time from the last lap of the stint
        MAX(l.date_start) as stint_end_time
    FROM {{ ref('stg_f1_stints') }} s
    LEFT JOIN {{ ref('stg_f1_laps') }} l
        ON s.session_key = l.session_key
        AND s.driver_number = l.driver_number
        AND l.lap_number BETWEEN s.lap_start AND s.lap_end
    GROUP BY 1,2,3,4,5,6