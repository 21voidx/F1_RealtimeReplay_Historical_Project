with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        driver_number::number as driver_number,
        {{ nullif_text('broadcast_name') }} as broadcast_name,
        {{ nullif_text('first_name') }} as first_name,
        {{ nullif_text('last_name') }} as last_name,
        {{ nullif_text('full_name') }} as full_name,
        {{ nullif_text('name_acronym') }} as name_acronym,
        {{ nullif_text('headshot_url') }} as headshot_url,
        {{ nullif_text('team_name') }} as team_name,
        {{ colour_to_hex('team_colour') }} as team_colour_hex,
        {{ nullif_text('country_code') }} as country_code
    from {{ source('openf1_raw', 'raw_drivers') }}
    where session_key is not null
      and driver_number is not null
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number']) }} as session_driver_id,
        *
    from source
)

select *
from renamed
qualify row_number() over (
    partition by session_key, driver_number
    order by full_name nulls last, team_name nulls last
) = 1
