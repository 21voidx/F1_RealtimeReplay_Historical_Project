{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='position_id',
    cluster_by=['session_key', 'driver_number', 'position_at'],
    on_schema_change='sync_all_columns',
    tags=['large', 'position']
) }}

with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        driver_number::number as driver_number,
        date::timestamp_ntz as position_at,
        position::number as position
    from {{ source('openf1_raw', 'raw_position') }}
    where session_key is not null
      and driver_number is not null
      and date is not null
      {% if is_incremental() %}
      and date >= dateadd(day, -{{ var('incremental_lookback_days', 3) }}, coalesce((select max(position_at) from {{ this }}), '1900-01-01'::timestamp_ntz))
      {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', 'position_at']) }} as position_id,
        meeting_key,
        session_key,
        driver_number,
        position_at,
        position,
        current_timestamp()::timestamp_ntz as dbt_loaded_at
    from source
)

select *
from renamed
qualify row_number() over (
    partition by position_id
    order by position_at desc
) = 1
