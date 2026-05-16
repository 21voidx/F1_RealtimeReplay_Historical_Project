{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number']) }} as telemetry_driver_session_id,
    meeting_key,
    session_key,
    driver_number,
    season_year,
    country_name,
    circuit_short_name,
    session_name,
    session_type,
    full_name,
    name_acronym,
    team_name,
    count(*) as observed_laps,
    avg(avg_speed) as session_avg_speed,
    max(max_speed) as session_max_speed,
    avg(avg_throttle) as session_avg_throttle,
    avg(brake_usage_pct) as session_avg_brake_usage_pct,
    avg(drs_active_pct) as session_avg_drs_active_pct,
    avg(full_throttle_pct) as session_avg_full_throttle_pct,
    sum(telemetry_sample_count) as telemetry_sample_count
from {{ ref('fct_openf1__telemetry_lap_driver') }}
group by 1,2,3,4,5,6,7,8,9,10,11,12
