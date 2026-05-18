select
    session_key::number as session_key,
    meeting_key::number as meeting_key,
    {{ nullif_text('country_name') }} as country_name,
    {{ nullif_text('country_code') }} as country_code,
    {{ nullif_text('session_name') }} as session_name,
    {{ nullif_text('session_type') }} as session_type,
    year::number as season_year,
    circuit_key::number as circuit_key,
    {{ nullif_text('circuit_short_name') }} as circuit_short_name,
    date_start::timestamp_ntz as session_start_at,
    date_end::timestamp_ntz as session_end_at,
    {{ nullif_text('location') }} as location,
    coalesce(is_cancelled, false) as is_cancelled,
    concat_ws(' - ', year::string, country_name, session_name) as session_display_name
from {{ source('openf1_raw', 'raw_sessions') }}
where session_key is not null
qualify row_number() over (
    partition by session_key
    order by date_start desc nulls last
) = 1
