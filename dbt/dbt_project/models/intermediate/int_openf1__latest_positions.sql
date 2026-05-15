select
    meeting_key,
    session_key,
    driver_number,
    position as final_position,
    timestamp_utc as final_position_at
from {{ ref('stg_openf1__position') }}
qualify row_number() over (
    partition by session_key, driver_number
    order by timestamp_utc desc
) = 1
