{{ config(materialized='table') }}

select
    meeting_key,
    season_year,
    meeting_name,
    meeting_official_name,
    country_name,
    location,
    circuit_key,
    circuit_short_name,
    circuit_type,
    circuit_code,
    circuit_flag,
    circuit_image,
    circuit_info_url,
    meeting_start_at,
    meeting_end_at,
    is_cancelled
from {{ ref('stg_openf1__meetings') }}
