{{ config(materialized='table') }}

with sector_laps as (
    select meeting_key, season_year, country_name, circuit_short_name, session_key, session_name, session_type,
           driver_number, full_name, name_acronym, team_name, 1 as sector_number, sector_1_seconds as sector_seconds
    from {{ ref('int_openf1__lap_enriched') }}
    where is_valid_sector_time
    union all
    select meeting_key, season_year, country_name, circuit_short_name, session_key, session_name, session_type,
           driver_number, full_name, name_acronym, team_name, 2 as sector_number, sector_2_seconds as sector_seconds
    from {{ ref('int_openf1__lap_enriched') }}
    where is_valid_sector_time
    union all
    select meeting_key, season_year, country_name, circuit_short_name, session_key, session_name, session_type,
           driver_number, full_name, name_acronym, team_name, 3 as sector_number, sector_3_seconds as sector_seconds
    from {{ ref('int_openf1__lap_enriched') }}
    where is_valid_sector_time
),

race as (
    select
        meeting_key,
        season_year,
        country_name,
        circuit_short_name,
        driver_number,
        full_name,
        name_acronym,
        team_name,
        sector_number,
        avg(sector_seconds) as race_avg_sector_seconds,
        min(sector_seconds) as race_best_sector_seconds,
        count(*) as race_sector_lap_count
    from sector_laps
    where session_type in ('Race', 'Sprint')
    group by 1,2,3,4,5,6,7,8,9
),

qualifying as (
    select
        meeting_key,
        driver_number,
        sector_number,
        avg(sector_seconds) as qualifying_avg_sector_seconds,
        min(sector_seconds) as qualifying_best_sector_seconds,
        count(*) as qualifying_sector_lap_count
    from sector_laps
    where session_type in ('Qualifying', 'Sprint Qualifying', 'Sprint Shootout')
    group by 1,2,3
)

select
    {{ dbt_utils.generate_surrogate_key(['r.meeting_key', 'r.driver_number', 'r.sector_number']) }} as race_qualifying_sector_id,
    r.*,
    q.qualifying_avg_sector_seconds,
    q.qualifying_best_sector_seconds,
    q.qualifying_sector_lap_count,
    r.race_avg_sector_seconds - q.qualifying_best_sector_seconds as avg_race_vs_best_qualifying_delta_seconds,
    r.race_best_sector_seconds - q.qualifying_best_sector_seconds as best_race_vs_best_qualifying_delta_seconds,
    iff(q.qualifying_best_sector_seconds = 0, null, (r.race_avg_sector_seconds - q.qualifying_best_sector_seconds) / q.qualifying_best_sector_seconds) as avg_race_vs_qualifying_delta_pct
from race r
left join qualifying q
    on r.meeting_key = q.meeting_key
   and r.driver_number = q.driver_number
   and r.sector_number = q.sector_number
