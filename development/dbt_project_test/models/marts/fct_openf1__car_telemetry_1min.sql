{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='telemetry_minute_id',
    cluster_by=['event_date', 'session_key', 'driver_number'],
    on_schema_change='sync_all_columns',
    tags=['mart', 'gold', 'high_volume', 'telemetry']
) }}

with telemetry as (
    select *
    from {{ ref('stg_openf1__car_data') }}
    where timestamp_utc is not null
      {{ incremental_timestamp_predicate('timestamp_utc', 'sample_minute') }}
),

rolled_up as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', "date_trunc('minute', timestamp_utc)"]) }} as telemetry_minute_id,
        meeting_key,
        session_key,
        driver_number,
        date_trunc('minute', timestamp_utc) as sample_minute,
        to_date(date_trunc('minute', timestamp_utc)) as event_date,
        count(*) as sample_count,
        avg(speed) as avg_speed,
        max(speed) as max_speed,
        avg(rpm) as avg_rpm,
        max(rpm) as max_rpm,
        avg(throttle) as avg_throttle,
        avg(brake) as avg_brake,
        max(drs) as max_drs_state,
        max(n_gear) as max_gear,
        min(n_gear) as min_gear
    from telemetry
    group by 1, 2, 3, 4, 5, 6
)

select
    r.*,
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
from rolled_up r
left join {{ ref('dim_openf1__drivers') }} d
    on r.session_key = d.session_key
   and r.driver_number = d.driver_number
left join {{ ref('dim_openf1__sessions') }} s
    on r.session_key = s.session_key
