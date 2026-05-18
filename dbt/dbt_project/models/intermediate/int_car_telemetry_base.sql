SELECT
   ts.driver_number,
   ts.session_key,
   d.name_acronym,
   d.team_name,
   d.team_colour,
   ts.timestamp,
   ts.rpm,
   ts.speed,
   ts.throttle,
   ts.brake,
   ts.drs
FROM {{ ref('stg_f1_car_data') }} ts
LEFT JOIN {{ ref('stg_f1_drivers') }} d
   ON ts.driver_number::INTEGER = d.driver_number
   AND ts.session_key = d.session_key