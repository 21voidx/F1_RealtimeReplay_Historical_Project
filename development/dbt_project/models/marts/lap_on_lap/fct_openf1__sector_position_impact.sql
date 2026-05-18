{{ config(materialized='table', cluster_by=['session_key', 'lap_number']) }}

with base as (
    select
        g.*,
        avg(sector_1_seconds) over (partition by session_key, lap_number) as field_avg_sector_1_seconds,
        avg(sector_2_seconds) over (partition by session_key, lap_number) as field_avg_sector_2_seconds,
        avg(sector_3_seconds) over (partition by session_key, lap_number) as field_avg_sector_3_seconds,
        avg(lap_duration_seconds) over (partition by session_key, lap_number) as field_avg_lap_seconds
    from {{ ref('fct_openf1__lap_gap_evolution') }} g
    where is_valid_lap_time
)

select
    lap_id,
    meeting_key,
    session_key,
    driver_number,
    season_year,
    country_name,
    circuit_short_name,
    session_name,
    session_type,
    full_name,
    name_acronym,
    team_name,
    lap_number,
    lap_start_at,
    lap_duration_seconds,
    sector_1_seconds,
    sector_2_seconds,
    sector_3_seconds,
    sector_1_seconds - field_avg_sector_1_seconds as sector_1_delta_to_field_seconds,
    sector_2_seconds - field_avg_sector_2_seconds as sector_2_delta_to_field_seconds,
    sector_3_seconds - field_avg_sector_3_seconds as sector_3_delta_to_field_seconds,
    lap_duration_seconds - field_avg_lap_seconds as lap_delta_to_field_seconds,
    position,
    previous_lap_position,
    position_delta,
    gap_to_leader_seconds,
    gap_delta_to_previous_lap_seconds
from base
