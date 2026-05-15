select
    meeting_key,
    session_key,
    driver_number,
    gap_to_leader_raw,
    gap_to_leader_seconds,
    interval_to_ahead_raw,
    interval_to_ahead_seconds,
    timestamp_utc as latest_interval_at
from {{ ref('stg_openf1__intervals') }}
qualify row_number() over (
    partition by session_key, driver_number
    order by timestamp_utc desc
) = 1
