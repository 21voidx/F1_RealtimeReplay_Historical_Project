with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        driver_number::number as driver_number,
        stint_number::number as stint_number,
        lap_start::number as lap_start,
        lap_end::number as lap_end,
        case
            when {{ nullif_text('compound') }} is null then 'UNKNOWN'
            else upper({{ nullif_text('compound') }})
        end as compound,
        tyre_age_at_start::number as tyre_age_at_start
    from {{ source('openf1_raw', 'raw_stints') }}
    where session_key is not null
      and driver_number is not null
      and stint_number is not null
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', 'stint_number']) }} as stint_id,
        meeting_key,
        session_key,
        driver_number,
        stint_number,
        lap_start,
        lap_end,
        case
            when lap_start is not null
             and lap_end is not null
             and lap_end >= lap_start
                then lap_end - lap_start + 1
            else null
        end as stint_lap_count,
        compound,
        tyre_age_at_start
    from source
)

select *
from renamed
qualify row_number() over (
    partition by session_key, driver_number, stint_number
    order by lap_end desc nulls last
) = 1
