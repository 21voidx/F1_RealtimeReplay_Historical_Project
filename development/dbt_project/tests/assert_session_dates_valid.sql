select *
from {{ ref('stg_openf1__sessions') }}
where session_start_at is not null
  and session_end_at is not null
  and session_end_at < session_start_at
