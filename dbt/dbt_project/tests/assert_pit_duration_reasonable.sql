select
    pit_stop_id,
    meeting_key,
    session_key,
    driver_number,
    pit_at,
    lap_number,
    raw_pit_duration_seconds,
    lane_duration_seconds,
    stop_duration_seconds,
    pit_stop_duration_seconds
from {{ ref('stg_openf1__pit') }}
where
    -- Negative duration is invalid.
    coalesce(raw_pit_duration_seconds, 0) < 0
    or coalesce(lane_duration_seconds, 0) < 0
    or coalesce(stop_duration_seconds, 0) < 0
    or coalesce(pit_stop_duration_seconds, 0) < 0

    -- Stationary pit stop duration should not be extremely high.
    -- This checks stop duration, not lane duration.
    or (
        stop_duration_seconds is not null
        and stop_duration_seconds > 120
    )