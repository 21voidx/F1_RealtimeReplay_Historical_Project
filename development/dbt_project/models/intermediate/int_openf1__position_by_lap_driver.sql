{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

with lap_position as (
    select
        l.lap_id,
        l.meeting_key,
        l.session_key,
        l.driver_number,
        l.lap_number,
        l.lap_start_at,
        l.lap_end_at,
        p.position_at,
        p.position
    from {{ ref('int_openf1__lap_enriched') }} l
    left join {{ ref('stg_openf1__position') }} p
        on l.session_key = p.session_key
       and l.driver_number = p.driver_number
       and p.position_at >= l.lap_start_at
       and p.position_at < l.lap_end_at
    qualify row_number() over (
        partition by l.lap_id
        order by p.position_at desc nulls last
    ) = 1
)

select
    lap_id,
    meeting_key,
    session_key,
    driver_number,
    lap_number,
    lap_start_at,
    lap_end_at,
    position_at,
    position,
    lag(position) over (
        partition by session_key, driver_number
        order by lap_number
    ) as previous_lap_position,
    lag(position) over (
        partition by session_key, driver_number
        order by lap_number
    ) - position as position_delta
from lap_position
