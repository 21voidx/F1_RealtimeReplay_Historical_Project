with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        driver_number::number as driver_number,
        date::timestamp_ntz as pit_at,
        lap_number::number as lap_number,
        pit_duration::float as pit_duration_seconds,
        lane_duration::float as lane_duration_seconds,
        stop_duration::float as stop_duration_seconds
    from {{ source('openf1_raw', 'raw_pit') }}
    where session_key is not null
      and driver_number is not null
      and date is not null
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', 'pit_at', 'lap_number']) }} as pit_stop_id,
        meeting_key,
        session_key,
        driver_number,
        pit_at,
        to_date(pit_at) as event_date,
        lap_number,
        pit_duration_seconds,
        lane_duration_seconds,
        stop_duration_seconds
    from source
)

select *
from renamed
qualify row_number() over (
    partition by pit_stop_id
    order by pit_at desc
) = 1
