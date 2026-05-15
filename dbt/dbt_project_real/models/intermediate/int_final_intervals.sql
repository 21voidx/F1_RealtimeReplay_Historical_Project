SELECT
    session_key,
    driver_number,
    MAX(CASE
        WHEN gap_to_leader LIKE '%L%' THEN NULL
        ELSE CAST(gap_to_leader AS FLOAT)
    END) AS gap_to_leader,
    MAX(CASE
        WHEN gap_to_next LIKE '%L%' THEN NULL
        ELSE CAST(gap_to_next AS FLOAT)
    END) AS gap_to_next
FROM {{ ref('stg_f1_intervals') }}
GROUP BY session_key, driver_number