select
    meeting_key,
    session_key,
    driver_number,
    count(*) as lap_count,
    min(lap_duration_seconds) as best_lap_seconds,
    avg(lap_duration_seconds) as avg_lap_seconds,
    max(lap_duration_seconds) as slowest_lap_seconds,
    min_by(lap_number, lap_duration_seconds) as best_lap_number,
    avg(sector_1_seconds) as avg_sector_1_seconds,
    avg(sector_2_seconds) as avg_sector_2_seconds,
    avg(sector_3_seconds) as avg_sector_3_seconds,
    max(greatest(coalesce(i1_speed, 0), coalesce(i2_speed, 0), coalesce(speed_trap, 0))) as max_lap_speed
from {{ ref('stg_openf1__laps') }}
where lap_duration_seconds is not null
  and lap_duration_seconds > 0
group by 1, 2, 3
