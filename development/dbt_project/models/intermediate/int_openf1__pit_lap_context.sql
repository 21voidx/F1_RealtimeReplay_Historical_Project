{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

select
    l.lap_id,
    l.meeting_key,
    l.session_key,
    l.driver_number,
    l.lap_number,
    l.lap_start_at,
    l.lap_duration_seconds,
    l.is_pit_out_lap,
    p.pit_stop_id,
    p.pit_at,
    p.lap_number as pit_lap_number,
    p.pit_stop_duration_seconds,
    p.lane_duration_seconds,
    p.raw_pit_duration_seconds,
    case when p.pit_stop_id is not null then true else false end as is_pit_in_lap,
    lead(l.is_pit_out_lap) over (
        partition by l.session_key, l.driver_number
        order by l.lap_number
    ) as next_lap_is_pit_out,
    lag(l.lap_duration_seconds) over (
        partition by l.session_key, l.driver_number
        order by l.lap_number
    ) as previous_lap_duration_seconds,
    lead(l.lap_duration_seconds) over (
        partition by l.session_key, l.driver_number
        order by l.lap_number
    ) as next_lap_duration_seconds
from {{ ref('int_openf1__lap_enriched') }} l
left join {{ ref('stg_openf1__pit') }} p
    on l.session_key = p.session_key
   and l.driver_number = p.driver_number
   and l.lap_number = p.lap_number
