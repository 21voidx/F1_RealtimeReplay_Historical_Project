select *
from {{ ref('fct_openf1__laps') }}
where coalesce(sector_1_seconds, 0) < 0
   or coalesce(sector_2_seconds, 0) < 0
   or coalesce(sector_3_seconds, 0) < 0
