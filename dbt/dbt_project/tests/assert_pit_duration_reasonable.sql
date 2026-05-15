select *
from {{ ref('stg_openf1__pit') }}
where pit_duration_seconds is not null
  and pit_duration_seconds > 180
