{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

select
    t.lap_id,
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
    l.is_pit_out_lap,
    t.telemetry_sample_count,
    t.avg_speed,
    t.max_speed,
    t.avg_throttle,
    t.max_throttle,
    t.avg_rpm,
    t.max_rpm,
    t.avg_gear,
    t.brake_sample_count,
    t.full_throttle_sample_count,
    t.drs_active_sample_count,
    t.drs_detected_sample_count,
    t.brake_usage_pct,
    t.full_throttle_pct,
    t.drs_active_pct,
    t.drs_detected_pct,
    current_timestamp()::timestamp_ntz as dbt_loaded_at
from {{ ref('int_openf1__telemetry_lap_driver') }} t
left join {{ ref('int_openf1__lap_enriched') }} l
    on t.lap_id = l.lap_id
where t.telemetry_sample_count >= {{ var('telemetry_min_sample_count', 3) }}
