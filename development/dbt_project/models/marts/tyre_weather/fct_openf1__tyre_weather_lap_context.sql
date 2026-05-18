{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

select
    l.lap_id,
    l.meeting_key,
    l.session_key,
    l.driver_number,
    l.season_year,
    l.country_name,
    l.circuit_short_name,
    l.session_name,
    l.session_type,
    l.full_name,
    l.name_acronym,
    l.team_name,
    l.team_colour_hex,
    l.lap_number,
    l.lap_start_at,
    l.lap_duration_seconds,
    l.is_valid_lap_time,
    l.is_pit_out_lap,
    st.stint_id,
    st.stint_number,
    st.stint_lap_start,
    st.stint_lap_end,
    st.stint_lap_count,
    st.compound,
    st.tyre_age_at_start,
    st.tyre_age_lap,
    w.weather_at,
    w.air_temperature,
    w.track_temperature,
    w.pressure,
    w.humidity,
    w.rainfall,
    w.wind_speed,
    w.wind_direction,
    w.weather_condition_bucket,
    case
        when w.track_temperature < 25 then 'TRACK_COLD'
        when w.track_temperature < 35 then 'TRACK_MODERATE'
        when w.track_temperature < 45 then 'TRACK_HOT'
        when w.track_temperature >= 45 then 'TRACK_VERY_HOT'
        else 'UNKNOWN'
    end as track_temp_bucket,
    case
        when w.humidity < 40 then 'HUMIDITY_LOW'
        when w.humidity < 70 then 'HUMIDITY_MEDIUM'
        when w.humidity >= 70 then 'HUMIDITY_HIGH'
        else 'UNKNOWN'
    end as humidity_bucket,
    current_timestamp()::timestamp_ntz as dbt_loaded_at
from {{ ref('int_openf1__lap_enriched') }} l
left join {{ ref('int_openf1__stint_lap_context') }} st
    on l.lap_id = st.lap_id
left join {{ ref('int_openf1__weather_by_lap') }} w
    on l.lap_id = w.lap_id
where l.session_type in ('Race', 'Sprint')
