with pit_stops as (
    select
        meeting_key,
        session_key,
        driver_number,
        pit_at,
        lap_number,
        pit_stop_duration_seconds,
        lane_duration_seconds,
        raw_pit_duration_seconds,
        has_stationary_stop_duration,
        is_valid_stationary_stop_duration,
        is_valid_lane_duration
    from {{ ref('stg_openf1__pit') }}
),

rollup as (
    select
        meeting_key,
        session_key,
        driver_number,

        count(*) as pit_event_count,

        count_if(has_stationary_stop_duration) as pit_stop_count,

        avg(
            case
                when is_valid_stationary_stop_duration
                    then pit_stop_duration_seconds
            end
        ) as avg_pit_duration_seconds,

        min(
            case
                when is_valid_stationary_stop_duration
                    then pit_stop_duration_seconds
            end
        ) as fastest_pit_duration_seconds,

        max(
            case
                when is_valid_stationary_stop_duration
                    then pit_stop_duration_seconds
            end
        ) as slowest_pit_duration_seconds,

        avg(
            case
                when is_valid_lane_duration
                    then lane_duration_seconds
            end
        ) as avg_pit_lane_duration_seconds,

        min(
            case
                when is_valid_lane_duration
                    then lane_duration_seconds
            end
        ) as fastest_pit_lane_duration_seconds,

        max(
            case
                when is_valid_lane_duration
                    then lane_duration_seconds
            end
        ) as slowest_pit_lane_duration_seconds,

        avg(raw_pit_duration_seconds) as avg_raw_pit_duration_seconds,

        listagg(lap_number::string, ', ') within group (order by lap_number) as pit_laps,

        min(pit_at) as first_pit_at,
        max(pit_at) as last_pit_at

    from pit_stops
    group by 1, 2, 3
)

select *
from rollup