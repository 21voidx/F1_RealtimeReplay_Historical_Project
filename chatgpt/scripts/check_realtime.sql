USE f1_realtime;
SELECT 'f1_car_data' AS table_name, COUNT(*) AS rows_count FROM f1_car_data
UNION ALL SELECT 'f1_location_data', COUNT(*) FROM f1_location_data
UNION ALL SELECT 'f1_position_data', COUNT(*) FROM f1_position_data
UNION ALL SELECT 'f1_intervals_data', COUNT(*) FROM f1_intervals_data;

SELECT * FROM f1_car_data ORDER BY created_at DESC LIMIT 10;
