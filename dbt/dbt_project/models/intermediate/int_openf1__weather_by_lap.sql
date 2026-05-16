{{ config(materialized='table', cluster_by=['session_key', 'lap_number']) }}

select
    l.lap_id,
    l.meeting_key,
    l.session_key,
    l.driver_number,
    l.lap_number,
    w.weather_at,
    w.air_temperature,
    w.track_temperature,
    w.pressure,
    w.humidity,
    w.rainfall,
    w.wind_speed,
    w.wind_direction,
    w.weather_condition_bucket
from {{ ref('int_openf1__lap_enriched') }} l
left join {{ ref('stg_openf1__weather') }} w
    on l.session_key = w.session_key
   and w.weather_at <= l.lap_end_at
qualify row_number() over (
    partition by l.lap_id
    order by w.weather_at desc nulls last
) = 1
