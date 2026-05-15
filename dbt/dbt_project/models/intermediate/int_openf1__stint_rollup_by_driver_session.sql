select
    meeting_key,
    session_key,
    driver_number,
    count(*) as stint_count,
    sum(stint_lap_count) as stint_lap_count_total,
    avg(stint_lap_count) as avg_stint_lap_count,
    listagg(distinct compound, ', ') within group (order by compound) as compounds_used
from {{ ref('stg_openf1__stints') }}
group by 1, 2, 3
