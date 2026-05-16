WITH tire_performance AS (
    SELECT
        circuit_short_name,
        compound,
        team_name,
        driver_number,
        AVG(stint_length) as avg_stint_length,
        MAX(stint_length) as max_stint_length,
        AVG(tyre_age_at_start) as avg_starting_age,
        COUNT(*) as number_of_stints
    FROM {{ ref('int_tyre_stints_enriched') }}
    GROUP BY 1,2,3,4
)

SELECT
    circuit_short_name,
    compound,
    AVG(avg_stint_length) as circuit_compound_avg_stint,
    MAX(max_stint_length) as circuit_compound_max_stint,
    AVG(avg_starting_age) as avg_tire_age_start,
    SUM(number_of_stints) as total_stints
FROM tire_performance
GROUP BY 1,2