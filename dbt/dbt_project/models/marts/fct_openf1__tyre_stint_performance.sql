{{ config(materialized='table', tags=['mart', 'gold', 'tyres']) }}

with stint_laps as (
    select
        st.stint_id,
        st.meeting_key,
        st.session_key,
        st.driver_number,
        st.stint_number,
        st.lap_start,
        st.lap_end,
        st.stint_lap_count,
        st.compound,
        st.tyre_age_at_start,
        avg(l.lap_duration_seconds) as avg_lap_seconds_in_stint,
        min(l.lap_duration_seconds) as best_lap_seconds_in_stint,
        max(l.max_lap_speed) as max_speed_in_stint,
        count(l.lap_id) as observed_lap_count
    from {{ ref('stg_openf1__stints') }} st
    left join {{ ref('fct_openf1__laps') }} l
        on st.session_key = l.session_key
       and st.driver_number = l.driver_number
       and l.lap_number between st.lap_start and st.lap_end
    group by 1,2,3,4,5,6,7,8,9,10
)

select
    sl.*,
    d.full_name,
    d.name_acronym,
    d.team_name,
    d.team_colour_hex,
    s.season_year,
    s.country_name,
    s.circuit_short_name,
    s.session_name,
    s.session_type,
    current_timestamp()::timestamp_ntz as dbt_loaded_at
from stint_laps sl
left join {{ ref('dim_openf1__drivers') }} d
    on sl.session_key = d.session_key
   and sl.driver_number = d.driver_number
left join {{ ref('dim_openf1__sessions') }} s
    on sl.session_key = s.session_key
