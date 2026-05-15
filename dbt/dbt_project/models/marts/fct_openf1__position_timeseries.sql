{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='position_minute_id',
    cluster_by=['event_date', 'session_key', 'driver_number'],
    on_schema_change='sync_all_columns',
    tags=['mart', 'gold', 'position']
) }}

with positions as (
    select *
    from {{ ref('stg_openf1__position') }}
    where timestamp_utc is not null
      {{ incremental_timestamp_predicate('timestamp_utc', 'position_minute') }}
),

minute_snapshots as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', "date_trunc('minute', timestamp_utc)"]) }} as position_minute_id,
        meeting_key,
        session_key,
        driver_number,
        date_trunc('minute', timestamp_utc) as position_minute,
        to_date(date_trunc('minute', timestamp_utc)) as event_date,
        position,
        timestamp_utc as source_timestamp_utc
    from positions
    qualify row_number() over (
        partition by session_key, driver_number, date_trunc('minute', timestamp_utc)
        order by timestamp_utc desc
    ) = 1
)

select
    p.*,
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
from minute_snapshots p
left join {{ ref('dim_openf1__drivers') }} d
    on p.session_key = d.session_key
   and p.driver_number = d.driver_number
left join {{ ref('dim_openf1__sessions') }} s
    on p.session_key = s.session_key
