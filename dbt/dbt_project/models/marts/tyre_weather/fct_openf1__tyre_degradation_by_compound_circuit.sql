{{ config(materialized='table') }}

with valid_laps as (
    select *
    from {{ ref('fct_openf1__tyre_weather_lap_context') }}
    where is_valid_lap_time
      and not is_pit_out_lap
      and compound is not null
      and tyre_age_lap is not null
),

age_curve as (
    select
        season_year,
        circuit_short_name,
        compound,
        tyre_age_lap,
        count(*) as sample_laps,
        count(distinct driver_number) as sample_drivers,
        count(distinct session_key) as sample_sessions,
        avg(lap_duration_seconds) as avg_lap_seconds,
        min(lap_duration_seconds) as best_lap_seconds,
        median(lap_duration_seconds) as median_lap_seconds,
        avg(track_temperature) as avg_track_temperature,
        avg(humidity) as avg_humidity,
        max({{ bool_to_int('rainfall') }}) as has_rainfall
    from valid_laps
    group by 1,2,3,4
),

with_baseline as (
    select
        *,
        min(avg_lap_seconds) over (partition by season_year, circuit_short_name, compound) as best_avg_lap_seconds_for_compound,
        avg_lap_seconds - min(avg_lap_seconds) over (partition by season_year, circuit_short_name, compound) as degradation_vs_best_age_seconds
    from age_curve
)

select
    {{ dbt_utils.generate_surrogate_key(['season_year', 'circuit_short_name', 'compound', 'tyre_age_lap']) }} as tyre_degradation_id,
    *
from with_baseline
where sample_laps >= {{ var('tyre_min_sample_laps', 5) }}
