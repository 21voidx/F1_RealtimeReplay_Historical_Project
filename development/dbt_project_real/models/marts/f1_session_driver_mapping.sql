SELECT DISTINCT
    d.session_key,
    concat(d.name_acronym, ' - ', d.full_name, ' (', d.team_name, ')') as driver_display_name,
    d.driver_number
FROM {{ ref('stg_f1_drivers') }} d
JOIN {{ ref('stg_f1_sessions') }} s ON d.session_key = s.session_key