WITH stints_base AS (
    SELECT
        s.*,
        d.team_name,
        ses.circuit_short_name,
        ses.country_name,
        ses.session_type
    FROM {{ ref('stg_f1_stints') }} s
    LEFT JOIN {{ ref('stg_f1_drivers') }} d ON s.driver_number = d.driver_number
        AND s.session_key = d.session_key
    LEFT JOIN {{ ref('stg_f1_sessions') }} ses ON s.session_key = ses.session_key
    WHERE ses.session_type = 'Race' -- Focus on race sessions
),

stint_durations AS (
    SELECT
        *,
        lap_end - lap_start + 1 as stint_length
        -- DATEDIFF('second', stint_start_time, stint_end_time) as stint_duration_seconds
    FROM stints_base
)

SELECT * FROM stint_durations