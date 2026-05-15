-- models/marts/f1_session_name_mapping.sql
SELECT DISTINCT
    concat(year, ' - ', country_name, ' - ', session_name) as session_display_name,
    session_key
FROM {{ ref('stg_f1_sessions') }}