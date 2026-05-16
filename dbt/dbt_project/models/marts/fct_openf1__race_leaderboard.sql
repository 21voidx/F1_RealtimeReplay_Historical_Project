{{ config(materialized='table', tags=['mart', 'gold', 'race']) }}

with driver_summary as (
    select
        session_driver_id,
        meeting_key,
        session_key,
        driver_number,

        full_name,
        name_acronym,
        team_name,
        team_colour_hex,

        season_year,
        country_name,
        circuit_short_name,
        session_name,
        session_type,
        session_start_at,
        session_end_at,

        lap_count,
        best_lap_seconds,
        best_lap_number,
        avg_lap_seconds,
        slowest_lap_seconds,
        avg_sector_1_seconds,
        avg_sector_2_seconds,
        avg_sector_3_seconds,

        max_speed,
        avg_speed,
        avg_rpm,

        pit_event_count,
        pit_stop_count,
        avg_pit_duration_seconds,
        fastest_pit_duration_seconds,
        slowest_pit_duration_seconds,
        avg_pit_lane_duration_seconds,
        fastest_pit_lane_duration_seconds,
        slowest_pit_lane_duration_seconds,
        avg_raw_pit_duration_seconds,
        pit_laps,
        first_pit_at,
        last_pit_at,

        stint_count,
        compounds_used,

        final_position,
        final_position_at,

        gap_to_leader_seconds,
        interval_to_ahead_seconds,

        dbt_loaded_at
    from {{ ref('fct_openf1__driver_session_summary') }}
    where session_type in ('Race', 'Sprint')
),

ranked as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'session_key',
            'driver_number'
        ]) }} as leaderboard_id,

        session_driver_id,
        meeting_key,
        session_key,
        driver_number,

        final_position,

        row_number() over (
            partition by session_key
            order by
                final_position nulls last,
                gap_to_leader_seconds nulls last,
                best_lap_seconds nulls last,
                driver_number
        ) as leaderboard_rank,

        case
            when final_position is not null then true
            else false
        end as has_final_position,

        full_name,
        name_acronym,
        team_name,
        team_colour_hex,

        season_year,
        country_name,
        circuit_short_name,
        session_name,
        session_type,
        session_start_at,
        session_end_at,

        lap_count,
        best_lap_seconds,
        best_lap_number,
        avg_lap_seconds,
        slowest_lap_seconds,
        avg_sector_1_seconds,
        avg_sector_2_seconds,
        avg_sector_3_seconds,

        max_speed,
        avg_speed,
        avg_rpm,

        coalesce(pit_event_count, 0) as pit_event_count,
        coalesce(pit_stop_count, 0) as pit_stop_count,
        avg_pit_duration_seconds,
        fastest_pit_duration_seconds,
        slowest_pit_duration_seconds,
        avg_pit_lane_duration_seconds,
        fastest_pit_lane_duration_seconds,
        slowest_pit_lane_duration_seconds,
        avg_raw_pit_duration_seconds,
        pit_laps,
        first_pit_at,
        last_pit_at,

        coalesce(stint_count, 0) as stint_count,
        compounds_used,

        gap_to_leader_seconds,
        interval_to_ahead_seconds,

        dbt_loaded_at
    from driver_summary
)

select *
from ranked