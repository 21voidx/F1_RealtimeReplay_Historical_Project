{{ config(materialized='table', tags=['mart', 'gold', 'summary']) }}

select
    d.session_driver_id,
    d.meeting_key,
    d.session_key,
    d.driver_number,
    d.full_name,
    d.name_acronym,
    d.team_name,
    d.team_colour_hex,
    s.season_year,
    s.country_name,
    s.circuit_short_name,
    s.session_name,
    s.session_type,
    s.session_start_at,
    s.session_end_at,
    coalesce(l.lap_count, 0) as lap_count,
    l.best_lap_seconds,
    l.best_lap_number,
    l.avg_lap_seconds,
    l.slowest_lap_seconds,
    l.avg_sector_1_seconds,
    l.avg_sector_2_seconds,
    l.avg_sector_3_seconds,
    greatest(coalesce(l.max_lap_speed, 0), coalesce(t.max_speed, 0)) as max_speed,
    t.avg_speed,
    t.avg_rpm,
    coalesce(p.pit_stop_count, 0) as pit_stop_count,
    p.avg_pit_duration_seconds,
    p.fastest_pit_duration_seconds,
    p.pit_laps,
    coalesce(st.stint_count, 0) as stint_count,
    st.compounds_used,
    pos.final_position,
    pos.final_position_at,
    i.gap_to_leader_seconds,
    i.interval_to_ahead_seconds,
    current_timestamp()::timestamp_ntz as dbt_loaded_at
from {{ ref('dim_openf1__drivers') }} d
left join {{ ref('dim_openf1__sessions') }} s
    on d.session_key = s.session_key
left join {{ ref('int_openf1__lap_rollup_by_driver_session') }} l
    on d.session_key = l.session_key
   and d.driver_number = l.driver_number
left join {{ ref('int_openf1__pit_rollup_by_driver_session') }} p
    on d.session_key = p.session_key
   and d.driver_number = p.driver_number
left join {{ ref('int_openf1__stint_rollup_by_driver_session') }} st
    on d.session_key = st.session_key
   and d.driver_number = st.driver_number
left join {{ ref('int_openf1__latest_positions') }} pos
    on d.session_key = pos.session_key
   and d.driver_number = pos.driver_number
left join {{ ref('int_openf1__latest_intervals') }} i
    on d.session_key = i.session_key
   and d.driver_number = i.driver_number
left join (
    select
        session_key,
        driver_number,
        max(max_speed) as max_speed,
        avg(avg_speed) as avg_speed,
        avg(avg_rpm) as avg_rpm
    from {{ ref('fct_openf1__car_telemetry_1min') }}
    group by 1, 2
) t
    on d.session_key = t.session_key
   and d.driver_number = t.driver_number
