{{ config(materialized='table') }}

select
    d.session_driver_id,
    d.meeting_key,
    d.session_key,
    d.driver_number,
    d.broadcast_name,
    d.first_name,
    d.last_name,
    d.full_name,
    d.name_acronym,
    d.headshot_url,
    d.team_name,
    d.team_colour_hex,
    d.country_code,
    s.season_year,
    s.session_type,
    s.session_display_name
from {{ ref('stg_openf1__drivers') }} d
left join {{ ref('dim_openf1__sessions') }} s
    on d.session_key = s.session_key
