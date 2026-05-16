with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        driver_number::number as driver_number,
        date_start::timestamp_ntz as lap_start_at,
        lap_duration::float as lap_duration_seconds,
        lap_number::number as lap_number,
        is_pit_out_lap::boolean as is_pit_out_lap,
        duration_sector_1::float as sector_1_seconds,
        duration_sector_2::float as sector_2_seconds,
        duration_sector_3::float as sector_3_seconds,
        i1_speed::number as i1_speed,
        i2_speed::number as i2_speed,
        st_speed::number as speed_trap,
        segments_sector_1,
        segments_sector_2,
        segments_sector_3
    from {{ source('openf1_raw', 'raw_laps') }}
    where session_key is not null
      and driver_number is not null
      and lap_number is not null
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', 'lap_number']) }} as lap_id,
        meeting_key,
        session_key,
        driver_number,
        lap_number,
        lap_start_at,
        iff(lap_duration_seconds is not null, dateadd('millisecond', lap_duration_seconds * 1000, lap_start_at), null) as lap_end_at,
        to_date(lap_start_at) as event_date,
        lap_duration_seconds,
        sector_1_seconds,
        sector_2_seconds,
        sector_3_seconds,
        i1_speed,
        i2_speed,
        speed_trap,
        is_pit_out_lap,
        segments_sector_1,
        segments_sector_2,
        segments_sector_3
    from source
)

select *
from renamed
qualify row_number() over (
    partition by session_key, driver_number, lap_number
    order by lap_start_at desc nulls last
) = 1
