{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

select
    l.lap_id,
    l.meeting_key,
    l.session_key,
    l.driver_number,
    l.season_year,
    l.country_name,
    l.circuit_short_name,
    l.session_name,
    l.session_type,
    l.full_name,
    l.name_acronym,
    l.team_name,
    l.team_colour_hex,
    l.lap_number,
    l.lap_start_at,
    l.lap_end_at,
    l.lap_duration_seconds,
    l.is_valid_lap_time,
    l.is_pit_out_lap,
    p.is_pit_in_lap,
    p.pit_stop_duration_seconds,
    p.lane_duration_seconds,
    pos.position,
    pos.previous_lap_position,
    pos.position_delta,
    i.gap_to_leader_seconds,
    i.interval_to_ahead_seconds,
    i.gap_delta_to_previous_lap_seconds,
    i.is_lapped_to_leader,
    i.is_lapped_to_ahead,
    l.sector_1_seconds,
    l.sector_2_seconds,
    l.sector_3_seconds,
    l.max_lap_speed,
    current_timestamp()::timestamp_ntz as dbt_loaded_at
from {{ ref('int_openf1__lap_enriched') }} l
left join {{ ref('int_openf1__position_by_lap_driver') }} pos
    on l.lap_id = pos.lap_id
left join {{ ref('int_openf1__intervals_by_lap_driver') }} i
    on l.lap_id = i.lap_id
left join {{ ref('int_openf1__pit_lap_context') }} p
    on l.lap_id = p.lap_id
where l.session_type in ('Race', 'Sprint')
