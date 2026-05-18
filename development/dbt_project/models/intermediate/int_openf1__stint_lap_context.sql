{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

select
    l.lap_id,
    l.meeting_key,
    l.session_key,
    l.driver_number,
    l.lap_number,
    st.stint_id,
    st.stint_number,
    st.lap_start as stint_lap_start,
    st.lap_end as stint_lap_end,
    st.stint_lap_count,
    st.compound,
    st.tyre_age_at_start,
    case
        when st.tyre_age_at_start is not null and st.lap_start is not null
            then st.tyre_age_at_start + (l.lap_number - st.lap_start)
        when st.lap_start is not null
            then l.lap_number - st.lap_start + 1
        else null
    end as tyre_age_lap
from {{ ref('int_openf1__lap_enriched') }} l
left join {{ ref('stg_openf1__stints') }} st
    on l.session_key = st.session_key
   and l.driver_number = st.driver_number
   and l.lap_number between st.lap_start and st.lap_end
qualify row_number() over (
    partition by l.lap_id
    order by st.stint_number desc nulls last
) = 1
