{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='weather_5min_id',
    cluster_by=['event_date', 'session_key'],
    on_schema_change='sync_all_columns',
    tags=['mart', 'gold', 'weather']
) }}

with weather as (
    select *
    from {{ ref('stg_openf1__weather') }}
    where timestamp_utc is not null
      {{ incremental_timestamp_predicate('timestamp_utc', 'weather_5min') }}
),

rolled_up as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', "time_slice(timestamp_utc, 5, 'minute', 'start')"]) }} as weather_5min_id,
        meeting_key,
        session_key,
        time_slice(timestamp_utc, 5, 'minute', 'start') as weather_5min,
        to_date(time_slice(timestamp_utc, 5, 'minute', 'start')) as event_date,
        avg(air_temperature) as avg_air_temperature,
        avg(track_temperature) as avg_track_temperature,
        avg(pressure) as avg_pressure,
        avg(humidity) as avg_humidity,
        max(iff(rainfall, 1, 0)) as has_rainfall,
        avg(wind_speed) as avg_wind_speed,
        avg(wind_direction) as avg_wind_direction,
        count(*) as weather_sample_count
    from weather
    group by 1, 2, 3, 4, 5
)

select
    w.*,
    s.season_year,
    s.country_name,
    s.circuit_short_name,
    s.session_name,
    s.session_type,
    current_timestamp()::timestamp_ntz as dbt_loaded_at
from rolled_up w
left join {{ ref('dim_openf1__sessions') }} s
    on w.session_key = s.session_key
