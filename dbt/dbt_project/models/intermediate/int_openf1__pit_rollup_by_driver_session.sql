select
    meeting_key,
    session_key,
    driver_number,
    count(*) as pit_stop_count,
    avg(pit_duration_seconds) as avg_pit_duration_seconds,
    min(pit_duration_seconds) as fastest_pit_duration_seconds,
    max(pit_duration_seconds) as slowest_pit_duration_seconds,
    listagg(lap_number::string, ', ') within group (order by lap_number) as pit_laps
from {{ ref('stg_openf1__pit') }}
group by 1, 2, 3
