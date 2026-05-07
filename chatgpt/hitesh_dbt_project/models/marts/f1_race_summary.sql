-- models/marts/f1_race_summary.sql
WITH positions AS (
   SELECT
       session_key,
       COUNT(DISTINCT driver_number) as total_drivers
       -- COUNT(DISTINCT CASE WHEN position > 20 THEN driver_number END) as retirements
   FROM {{ ref('stg_f1_position') }}
   GROUP BY session_key
),
laps AS (
   SELECT
       session_key,
       MAX(lap_number) as total_laps
   FROM {{ ref('stg_f1_laps') }}
   GROUP BY session_key
),
team_stats AS (
   SELECT
       session_key,
       team_name,
       COUNT(DISTINCT driver_number) as cars_finished,
       SUM(points) as constructor_points,
       SUM(pit_stop_count) as total_pit_stops
   FROM {{ ref('f1_race_leaderboard') }}
   GROUP BY session_key, team_name
),
driver_stats AS (
   SELECT
       session_key,
       driver_number,
       MAX(speed) as top_speed,
       AVG(speed) as avg_speed,
       SUM(CASE WHEN drs = 1 THEN 1 ELSE 0 END) as drs_activations
   FROM {{ ref('stg_f1_car_data') }}
   GROUP BY session_key, driver_number
)
SELECT
   p.session_key,
   m.session_display_name,
   p.total_drivers,
   --p.retirements,
   l.total_laps,
   COUNT(DISTINCT t.team_name) as total_teams,
   SUM(t.total_pit_stops) as total_pit_stops,
   MAX(d.top_speed) as fastest_speed,
   AVG(d.avg_speed) as average_speed
   -- SUM(d.drs_activations) as total_drs_activations
FROM positions p
JOIN laps l ON p.session_key = l.session_key
JOIN {{ ref('f1_session_name_mapping') }} m ON p.session_key = m.session_key
LEFT JOIN team_stats t ON p.session_key = t.session_key
LEFT JOIN driver_stats d ON p.session_key = d.session_key
GROUP BY p.session_key, m.session_display_name, p.total_drivers, l.total_laps