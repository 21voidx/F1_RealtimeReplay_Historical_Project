WITH session_drivers AS (
   SELECT DISTINCT
       s.session_key,
       s.country_name,
       s.session_name,
       s.year,
       d.driver_number,
       concat(d.name_acronym, ' - ', d.full_name, ' (', d.team_name, ')') as driver_display_name
   FROM {{ ref('stg_f1_sessions') }} s
   JOIN {{ ref('stg_f1_drivers') }} d ON s.session_key = d.session_key
)
SELECT 
   s.year,
   s.country_name,
   s.circuit_short_name,
   s.session_name,
   s.session_type,
   concat(s.year, ' - ', s.country_name, ' - ', s.session_name) as session_display_name,
   array_agg(DISTINCT sd.driver_display_name) as available_driver_names,
   m.meeting_name,
   m.meeting_official_name,
   s.session_key,
   s.date_start
FROM {{ ref('stg_f1_sessions') }} s
JOIN session_drivers sd ON s.session_key = sd.session_key
JOIN {{ ref('stg_f1_meetings') }} m ON s.year = m.year 
   AND s.country_name = m.country_name
GROUP BY 1,2,3,4,5,6,8,9,10,11