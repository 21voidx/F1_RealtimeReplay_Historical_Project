{{ config(materialized='table') }}

with valid_laps as (
    select *
    from {{ ref('fct_openf1__tyre_weather_lap_context') }}
    where is_valid_lap_time
      and not is_pit_out_lap
      and compound is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['season_year', 'circuit_short_name', 'compound', 'track_temp_bucket', 'humidity_bucket', 'weather_condition_bucket']) }} as tyre_weather_impact_id,
    season_year,
    circuit_short_name,
    compound,
    track_temp_bucket,
    humidity_bucket,
    weather_condition_bucket,
    count(*) as sample_laps,
    count(distinct driver_number) as sample_drivers,
    count(distinct team_name) as sample_teams,
    avg(lap_duration_seconds) as avg_lap_seconds,
    min(lap_duration_seconds) as best_lap_seconds,
    median(lap_duration_seconds) as median_lap_seconds,
    avg(track_temperature) as avg_track_temperature,
    avg(air_temperature) as avg_air_temperature,
    avg(humidity) as avg_humidity,
    avg(tyre_age_lap) as avg_tyre_age_lap,
    max({{ bool_to_int('rainfall') }}) as has_rainfall,
    corr(humidity, lap_duration_seconds) as corr_humidity_lap_seconds,
    corr(track_temperature, lap_duration_seconds) as corr_track_temp_lap_seconds
from valid_laps
group by 1,2,3,4,5,6,7
