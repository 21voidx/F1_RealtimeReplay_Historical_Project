{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

with lap_interval as (
    select
        l.lap_id,
        l.meeting_key,
        l.session_key,
        l.driver_number,
        l.lap_number,
        l.lap_start_at,
        l.lap_end_at,
        i.interval_at,
        i.gap_to_leader_raw,
        i.interval_to_ahead_raw,
        i.gap_to_leader_seconds,
        i.interval_to_ahead_seconds,
        i.is_lapped_to_leader,
        i.is_lapped_to_ahead
    from {{ ref('int_openf1__lap_enriched') }} l
    left join {{ ref('stg_openf1__intervals') }} i
        on l.session_key = i.session_key
       and l.driver_number = i.driver_number
       and i.interval_at >= l.lap_start_at
       and i.interval_at < l.lap_end_at
    qualify row_number() over (
        partition by l.lap_id
        order by i.interval_at desc nulls last
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
    interval_at,
    gap_to_leader_raw,
    interval_to_ahead_raw,
    gap_to_leader_seconds,
    interval_to_ahead_seconds,
    is_lapped_to_leader,
    is_lapped_to_ahead,
    lag(gap_to_leader_seconds) over (
        partition by session_key, driver_number
        order by lap_number
    ) as previous_lap_gap_to_leader_seconds,
    gap_to_leader_seconds - lag(gap_to_leader_seconds) over (
        partition by session_key, driver_number
        order by lap_number
    ) as gap_delta_to_previous_lap_seconds
from lap_interval
