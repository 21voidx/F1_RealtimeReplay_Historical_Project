{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['st.session_key', 'st.driver_number', 'st.stint_number']) }} as team_tyre_strategy_id,
    st.meeting_key,
    st.session_key,
    st.driver_number,
    l.season_year,
    l.country_name,
    l.circuit_short_name,
    l.session_name,
    l.session_type,
    l.full_name,
    l.name_acronym,
    l.team_name,
    st.stint_number,
    st.lap_start,
    st.lap_end,
    st.stint_lap_count,
    st.compound,
    st.tyre_age_at_start,
    avg(case when l.is_valid_lap_time and not l.is_pit_out_lap then l.lap_duration_seconds end) as avg_clean_lap_seconds_in_stint,
    min(case when l.is_valid_lap_time and not l.is_pit_out_lap then l.lap_duration_seconds end) as best_clean_lap_seconds_in_stint,
    count(case when l.is_valid_lap_time then 1 end) as observed_valid_laps,
    avg(w.track_temperature) as avg_track_temperature,
    avg(w.air_temperature) as avg_air_temperature,
    avg(w.humidity) as avg_humidity,
    max({{ bool_to_int('w.rainfall') }}) as has_rainfall
from {{ ref('stg_openf1__stints') }} st
left join {{ ref('int_openf1__lap_enriched') }} l
    on st.session_key = l.session_key
   and st.driver_number = l.driver_number
   and l.lap_number between st.lap_start and st.lap_end
left join {{ ref('int_openf1__weather_by_lap') }} w
    on l.lap_id = w.lap_id
where l.session_type in ('Race', 'Sprint')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
