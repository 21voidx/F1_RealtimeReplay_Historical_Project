SELECT
        st.session_key,
        st.driver_number,
        st.stint_number,
        st.compound,
        st.lap_start,
        st.lap_end,
        st.stint_length,
        -- Average weather conditions during the stint
        AVG(w.air_temperature) as avg_air_temp,
        AVG(w.track_temperature) as avg_track_temp,
        AVG(w.humidity) as avg_humidity,
        MAX(w.rainfall) as had_rainfall,  -- If there was any rain during stint
        AVG(w.wind_speed) as avg_wind_speed,
        -- Categorize conditions
        CASE
            WHEN AVG(w.track_temperature) < 25 THEN 'Cold'
            WHEN AVG(w.track_temperature) < 35 THEN 'Medium'
            ELSE 'Hot'
        END as track_temp_category,
        CASE
            WHEN MAX(w.rainfall) > 0 THEN 'Wet'
            WHEN AVG(w.humidity) > 70 THEN 'High Humidity'
            ELSE 'Dry'
        END as weather_condition
    FROM {{ref('int_tyre_stints_laps')}} st
    LEFT JOIN {{ ref('stg_f1_weather') }} w
        ON st.session_key = w.session_key
        AND w.date BETWEEN st.stint_start_time AND st.stint_end_time
    GROUP BY 1,2,3,4,5,6,7