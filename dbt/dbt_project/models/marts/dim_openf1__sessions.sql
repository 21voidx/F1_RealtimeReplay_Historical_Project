{{ config(materialized='table') }}

select
    s.session_key,
    s.meeting_key,
    s.season_year,
    coalesce(s.country_name, m.country_name) as country_name,
    coalesce(s.circuit_short_name, m.circuit_short_name) as circuit_short_name,
    s.session_name,
    s.session_type,
    s.session_start_at,
    s.session_end_at,
    s.gmt_offset,
    s.is_cancelled,
    s.location,
    m.meeting_name,
    m.meeting_official_name,
    concat(s.season_year, ' - ', coalesce(s.country_name, m.country_name), ' - ', s.session_name) as session_display_name
from {{ ref('stg_openf1__sessions') }} s
left join {{ ref('stg_openf1__meetings') }} m
    on s.meeting_key = m.meeting_key
