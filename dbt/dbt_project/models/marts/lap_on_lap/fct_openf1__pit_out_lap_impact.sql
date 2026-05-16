{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

with base as (
    select
        g.*,
        avg(case when is_valid_lap_time and not is_pit_out_lap and coalesce(is_pit_in_lap, false) = false then lap_duration_seconds end)
            over (partition by session_key, driver_number) as driver_clean_air_avg_lap_seconds,
        avg(case when is_valid_lap_time and not is_pit_out_lap and coalesce(is_pit_in_lap, false) = false then lap_duration_seconds end)
            over (partition by session_key, lap_number) as field_clean_avg_lap_seconds
    from {{ ref('fct_openf1__lap_gap_evolution') }} g
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
    is_pit_in_lap,
    is_pit_out_lap,
    pit_stop_duration_seconds,
    lane_duration_seconds,
    position,
    previous_lap_position,
    position_delta,
    gap_to_leader_seconds,
    gap_delta_to_previous_lap_seconds,
    driver_clean_air_avg_lap_seconds,
    field_clean_avg_lap_seconds,
    case when is_pit_out_lap then lap_duration_seconds - driver_clean_air_avg_lap_seconds end as pit_out_loss_vs_driver_avg_seconds,
    case when is_pit_out_lap then lap_duration_seconds - field_clean_avg_lap_seconds end as pit_out_loss_vs_field_avg_seconds
from base
where is_pit_out_lap or coalesce(is_pit_in_lap, false)
