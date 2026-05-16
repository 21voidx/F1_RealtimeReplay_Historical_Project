select *
from {{ ref('stg_openf1__pit') }}
where coalesce(raw_pit_duration_seconds, 0) < 0
   or coalesce(lane_duration_seconds, 0) < 0
   or coalesce(stop_duration_seconds, 0) < 0
   or coalesce(pit_stop_duration_seconds, 0) < 0
   or (stop_duration_seconds is not null and stop_duration_seconds > 120)
