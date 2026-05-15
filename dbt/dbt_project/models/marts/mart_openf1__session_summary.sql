{{ config(materialized='table', tags=['mart', 'gold', 'dashboard']) }}

with driver_summary as (
    select
        session_key,
        count(distinct driver_number) as driver_count,
        sum(lap_count) as total_laps,
        min(best_lap_seconds) as fastest_lap_seconds,
        max(max_speed) as max_speed,
        sum(pit_stop_count) as total_pit_stops
    from {{ ref('fct_openf1__driver_session_summary') }}
    group by 1
),

weather_summary as (
    select
        session_key,
        avg(avg_air_temperature) as avg_air_temperature,
        avg(avg_track_temperature) as avg_track_temperature,
        max(has_rainfall) as has_rainfall
    from {{ ref('fct_openf1__weather_5min') }}
    group by 1
)

select
    s.session_key,
    s.meeting_key,
    s.season_year,
    s.country_name,
    s.circuit_short_name,
    s.meeting_name,
    s.session_name,
    s.session_type,
    s.session_display_name,
    s.session_start_at,
    s.session_end_at,
    coalesce(ds.driver_count, 0) as driver_count,
    coalesce(ds.total_laps, 0) as total_laps,
    ds.fastest_lap_seconds,
    ds.max_speed,
    coalesce(ds.total_pit_stops, 0) as total_pit_stops,
    ws.avg_air_temperature,
    ws.avg_track_temperature,
    coalesce(ws.has_rainfall, 0) as has_rainfall,
    current_timestamp()::timestamp_ntz as dbt_loaded_at
from {{ ref('dim_openf1__sessions') }} s
left join driver_summary ds
    on s.session_key = ds.session_key
left join weather_summary ws
    on s.session_key = ws.session_key
