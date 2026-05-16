with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        driver_number::number as driver_number,
        date::timestamp_ntz as pit_at,
        lap_number::number as lap_number,

        -- OpenF1: pit_duration is deprecated and equals lane_duration.
        pit_duration::float as raw_pit_duration_seconds,

        -- Time spent in pit lane.
        lane_duration::float as lane_duration_seconds,

        -- Stationary time in pit box.
        stop_duration::float as stop_duration_seconds
    from {{ source('openf1_raw', 'raw_pit') }}
    where session_key is not null
      and driver_number is not null
      and date is not null
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'session_key',
            'driver_number',
            'pit_at',
            'lap_number'
        ]) }} as pit_stop_id,

        meeting_key,
        session_key,
        driver_number,
        pit_at,
        to_date(pit_at) as event_date,
        lap_number,

        raw_pit_duration_seconds,
        lane_duration_seconds,
        stop_duration_seconds,

        -- Main metric for pit stop analysis.
        stop_duration_seconds as pit_stop_duration_seconds,

        case
            when stop_duration_seconds is not null then true
            else false
        end as has_stationary_stop_duration,

        case
            when stop_duration_seconds is not null
             and stop_duration_seconds between 0 and 120
                then true
            else false
        end as is_valid_stationary_stop_duration,

        case
            when lane_duration_seconds is not null
             and lane_duration_seconds between 0 and 1800
                then true
            else false
        end as is_valid_lane_duration
    from source
)

select *
from renamed
qualify row_number() over (
    partition by pit_stop_id
    order by pit_at desc
) = 1