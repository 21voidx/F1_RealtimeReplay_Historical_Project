{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='car_data_id',
    cluster_by=['session_key', 'driver_number', 'telemetry_at'],
    on_schema_change='sync_all_columns',
    tags=['large', 'telemetry']
) }}

with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        driver_number::number as driver_number,
        date::timestamp_ntz as telemetry_at,
        rpm::number as rpm,
        speed::number as speed,
        throttle::number as throttle,
        brake::number as brake,
        n_gear::number as n_gear,
        drs::number as drs
    from {{ source('openf1_raw', 'raw_car_data') }}
    where session_key is not null
      and driver_number is not null
      and date is not null
      {% if is_incremental() %}
      and date >= dateadd(day, -{{ var('incremental_lookback_days', 3) }}, coalesce((select max(telemetry_at) from {{ this }}), '1900-01-01'::timestamp_ntz))
      {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', 'telemetry_at']) }} as car_data_id,
        meeting_key,
        session_key,
        driver_number,
        telemetry_at,
        rpm,
        speed,
        throttle,
        brake,
        n_gear,
        drs,
        {{ bool_to_int(is_drs_active('drs')) }} as is_drs_active,
        {{ bool_to_int(is_drs_detected('drs')) }} as is_drs_detected,
        {{ bool_to_int('brake > 0') }} as is_braking,
        {{ bool_to_int('throttle >= 95') }} as is_full_throttle,
        current_timestamp()::timestamp_ntz as dbt_loaded_at
    from source
)

select *
from renamed
qualify row_number() over (
    partition by car_data_id
    order by telemetry_at desc
) = 1
