{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='lap_id',
    cluster_by=['event_date', 'session_key', 'driver_number'],
    on_schema_change='sync_all_columns',
    tags=['mart', 'gold', 'laps']
) }}

with laps as (
    select *
    from {{ ref('stg_openf1__laps') }}
    where lap_start_at is not null
      {{ incremental_timestamp_predicate('lap_start_at', 'lap_start_at') }}
)

select
    l.lap_id,
    l.meeting_key,
    l.session_key,
    l.driver_number,
    d.full_name,
    d.name_acronym,
    d.team_name,
    d.team_colour_hex,
    s.season_year,
    s.country_name,
    s.circuit_short_name,
    s.session_name,
    s.session_type,
    l.lap_number,
    l.lap_start_at,
    l.lap_end_at,
    l.event_date,
    l.lap_duration_seconds,
    l.sector_1_seconds,
    l.sector_2_seconds,
    l.sector_3_seconds,
    l.i1_speed,
    l.i2_speed,
    l.speed_trap,
    greatest(coalesce(l.i1_speed, 0), coalesce(l.i2_speed, 0), coalesce(l.speed_trap, 0)) as max_lap_speed,
    l.is_pit_out_lap,
    current_timestamp()::timestamp_ntz as dbt_loaded_at
from laps l
left join {{ ref('dim_openf1__drivers') }} d
    on l.session_key = d.session_key
   and l.driver_number = d.driver_number
left join {{ ref('dim_openf1__sessions') }} s
    on l.session_key = s.session_key
