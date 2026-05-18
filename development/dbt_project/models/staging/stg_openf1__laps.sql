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
        st_speed::number as st_speed,
        segments_sector_1 as segments_sector_1,
        segments_sector_2 as segments_sector_2,
        segments_sector_3 as segments_sector_3
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
        lap_start_at,
        lead(lap_start_at) over (
            partition by session_key, driver_number
            order by lap_number
        ) as next_lap_start_at,
        lap_duration_seconds,
        lap_number,
        coalesce(is_pit_out_lap, false) as is_pit_out_lap,
        sector_1_seconds,
        sector_2_seconds,
        sector_3_seconds,
        i1_speed,
        i2_speed,
        st_speed,
        greatest_ignore_nulls(i1_speed, i2_speed, st_speed) as max_lap_speed,
        segments_sector_1,
        segments_sector_2,
        segments_sector_3,
        case
            when lap_duration_seconds between {{ var('valid_lap_min_seconds', 40) }} and {{ var('valid_lap_max_seconds', 300) }} then true
            else false
        end as is_valid_lap_time,
        case
            when sector_1_seconds between {{ var('valid_sector_min_seconds', 5) }} and {{ var('valid_sector_max_seconds', 120) }}
             and sector_2_seconds between {{ var('valid_sector_min_seconds', 5) }} and {{ var('valid_sector_max_seconds', 120) }}
             and sector_3_seconds between {{ var('valid_sector_min_seconds', 5) }} and {{ var('valid_sector_max_seconds', 120) }} then true
            else false
        end as is_valid_sector_time
    from source
)

select *
from renamed
qualify row_number() over (
    partition by session_key, driver_number, lap_number
    order by lap_start_at desc nulls last
) = 1
