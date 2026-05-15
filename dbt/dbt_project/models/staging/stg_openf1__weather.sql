{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='weather_id',
    cluster_by=['event_date', 'session_key'],
    on_schema_change='sync_all_columns',
    tags=['staging']
) }}

with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        date::timestamp_ntz as timestamp_utc,
        air_temperature::float as air_temperature,
        track_temperature::float as track_temperature,
        pressure::float as pressure,
        humidity::float as humidity,
        rainfall::boolean as rainfall,
        wind_speed::float as wind_speed,
        wind_direction::number as wind_direction
    from {{ source('openf1_raw', 'raw_weather') }}
    where date is not null
      and session_key is not null
      {{ incremental_timestamp_predicate('date', 'timestamp_utc') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['meeting_key', 'session_key', 'timestamp_utc']) }} as weather_id,
        meeting_key,
        session_key,
        timestamp_utc,
        to_date(timestamp_utc) as event_date,
        air_temperature,
        track_temperature,
        pressure,
        humidity,
        rainfall,
        wind_speed,
        wind_direction,
        current_timestamp()::timestamp_ntz as dbt_loaded_at
    from source
)

select *
from renamed
qualify row_number() over (
    partition by weather_id
    order by timestamp_utc desc
) = 1
