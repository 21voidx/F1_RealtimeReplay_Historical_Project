{{ config(materialized='table', tags=['mart', 'gold', 'race']) }}

select
    {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number']) }} as leaderboard_id,
    meeting_key,
    session_key,
    driver_number,
    final_position,
    row_number() over (
        partition by session_key
        order by final_position nulls last, best_lap_seconds nulls last
    ) as leaderboard_rank,
    full_name,
    name_acronym,
    team_name,
    team_colour_hex,
    season_year,
    country_name,
    circuit_short_name,
    session_name,
    lap_count,
    best_lap_seconds,
    best_lap_number,
    avg_lap_seconds,
    max_speed,
    pit_stop_count,
    compounds_used,
    gap_to_leader_seconds,
    interval_to_ahead_seconds,
    dbt_loaded_at
from {{ ref('fct_openf1__driver_session_summary') }}
where session_type in ('Race', 'Sprint')
