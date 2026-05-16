{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='weather_id',
    cluster_by=['session_key', 'weather_at'],
    on_schema_change='sync_all_columns',
    tags=['weather']
) }}

with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        date::timestamp_ntz as weather_at,
        air_temperature::float as air_temperature,
        track_temperature::float as track_temperature,
        pressure::float as pressure,
        humidity::float as humidity,
        rainfall::boolean as rainfall,
        wind_speed::float as wind_speed,
        wind_direction::number as wind_direction
    from {{ source('openf1_raw', 'raw_weather') }}
    where session_key is not null
      and date is not null
      {% if is_incremental() %}
      and date >= dateadd(day, -{{ var('incremental_lookback_days', 3) }}, coalesce((select max(weather_at) from {{ this }}), '1900-01-01'::timestamp_ntz))
      {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'weather_at']) }} as weather_id,
        meeting_key,
        session_key,
        weather_at,
        air_temperature,
        track_temperature,
        pressure,
        humidity,
        coalesce(rainfall, false) as rainfall,
        wind_speed,
        wind_direction,
        case
            when rainfall then 'RAIN'
            when humidity >= 80 then 'HUMID'
            when humidity >= 60 then 'MODERATE_HUMIDITY'
            else 'DRY'
        end as weather_condition_bucket,
        current_timestamp()::timestamp_ntz as dbt_loaded_at
    from source
)

select *
from renamed
qualify row_number() over (
    partition by weather_id
    order by weather_at desc
) = 1
