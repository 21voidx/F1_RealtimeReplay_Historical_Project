{{ config(materialized='table') }}

with curve as (
    select *
    from {{ ref('fct_openf1__tyre_degradation_by_compound_circuit') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by season_year, circuit_short_name, compound
            order by avg_lap_seconds asc, sample_laps desc, tyre_age_lap asc
        ) as optimal_life_rank
    from curve
)

select
    {{ dbt_utils.generate_surrogate_key(['season_year', 'circuit_short_name', 'compound']) }} as optimal_tyre_life_id,
    season_year,
    circuit_short_name,
    compound,
    tyre_age_lap as optimal_tyre_age_lap,
    sample_laps,
    sample_drivers,
    sample_sessions,
    avg_lap_seconds as optimal_age_avg_lap_seconds,
    best_lap_seconds as optimal_age_best_lap_seconds,
    median_lap_seconds as optimal_age_median_lap_seconds,
    avg_track_temperature,
    avg_humidity,
    has_rainfall
from ranked
where optimal_life_rank = 1
