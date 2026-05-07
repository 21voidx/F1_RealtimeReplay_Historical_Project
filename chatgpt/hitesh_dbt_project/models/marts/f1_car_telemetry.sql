-- models/marts/f1_car_telemetry.sql
SELECT 
   t.timestamp, 
   CONCAT(t.name_acronym, ' (', t.team_name, ')') as driver,
   t.driver_number,
   ROUND(t.speed) as speed,
   ROUND(t.throttle) as throttle,
   ROUND(t.brake) as brake,
   ROUND(t.drs) as drs,
   t.rpm,
   t.session_key,  -- Add session_key
   m.meeting_key   -- Add meeting_key
FROM {{ ref('int_car_telemetry_base') }} t
LEFT JOIN {{ ref('stg_f1_sessions') }} s ON t.session_key = s.session_key
LEFT JOIN {{ ref('stg_f1_meetings') }} m ON s.meeting_key = m.meeting_key