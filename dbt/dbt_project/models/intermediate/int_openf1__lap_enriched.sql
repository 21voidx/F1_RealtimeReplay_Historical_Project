{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

select
    l.lap_id,
    l.meeting_key,
    l.session_key,
    l.driver_number,
    l.lap_number,
    l.lap_start_at,
    l.next_lap_start_at,
    coalesce(l.next_lap_start_at, dateadd(minute, 5, l.lap_start_at)) as lap_end_at,
    l.lap_duration_seconds,
    l.is_pit_out_lap,
    l.sector_1_seconds,
    l.sector_2_seconds,
    l.sector_3_seconds,
    l.is_valid_lap_time,
    l.is_valid_sector_time,
    l.i1_speed,
    l.i2_speed,
    l.st_speed,
    l.max_lap_speed,
    s.season_year,
    s.country_name,
    s.circuit_short_name,
    s.session_name,
    s.session_type,
    s.session_start_at,
    s.session_end_at,
    d.session_driver_id,
    d.full_name,
    d.name_acronym,
    d.team_name,
    d.team_colour_hex
from {{ ref('stg_openf1__laps') }} l
left join {{ ref('stg_openf1__sessions') }} s
    on l.session_key = s.session_key
left join {{ ref('stg_openf1__drivers') }} d
    on l.session_key = d.session_key
   and l.driver_number = d.driver_number
