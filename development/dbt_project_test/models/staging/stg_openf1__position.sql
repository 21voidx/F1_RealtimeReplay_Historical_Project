{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='position_id',
    cluster_by=['event_date', 'session_key', 'driver_number'],
    on_schema_change='sync_all_columns',
    tags=['staging', 'high_volume']
) }}

with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        driver_number::number as driver_number,
        date::timestamp_ntz as timestamp_utc,
        position::number as position
    from {{ source('openf1_raw', 'raw_position') }}
    where date is not null
      and session_key is not null
      and driver_number is not null
      {{ incremental_timestamp_predicate('date', 'timestamp_utc') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['meeting_key', 'session_key', 'driver_number', 'timestamp_utc', 'position']) }} as position_id,
        meeting_key,
        session_key,
        driver_number,
        timestamp_utc,
        to_date(timestamp_utc) as event_date,
        position,
        current_timestamp()::timestamp_ntz as dbt_loaded_at
    from source
)

select *
from renamed
qualify row_number() over (
    partition by position_id
    order by timestamp_utc desc
) = 1
