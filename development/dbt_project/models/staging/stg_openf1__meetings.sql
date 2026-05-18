select
    meeting_key::number as meeting_key,
    {{ nullif_text('meeting_name') }} as meeting_name,
    {{ nullif_text('meeting_official_name') }} as meeting_official_name,
    {{ nullif_text('country_name') }} as country_name,
    circuit_key::number as circuit_key,
    {{ nullif_text('circuit_short_name') }} as circuit_short_name,
    {{ nullif_text('circuit_type') }} as circuit_type,
    {{ nullif_text('circuit_code') }} as circuit_code,
    year::number as season_year,
    date_start::timestamp_ntz as meeting_start_at,
    date_end::timestamp_ntz as meeting_end_at,
    {{ nullif_text('location') }} as location,
    coalesce(is_cancelled, false) as is_cancelled
from {{ source('openf1_raw', 'raw_meetings') }}
where meeting_key is not null
qualify row_number() over (
    partition by meeting_key
    order by date_start desc nulls last
) = 1
