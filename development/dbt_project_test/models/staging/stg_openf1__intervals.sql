{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='interval_id',
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
        {{ nullif_text('gap_to_leader') }} as gap_to_leader_raw,
        {{ nullif_text('interval') }} as interval_to_ahead_raw
    from {{ source('openf1_raw', 'raw_intervals') }}
    where date is not null
      and session_key is not null
      and driver_number is not null
      {{ incremental_timestamp_predicate('date', 'timestamp_utc') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['meeting_key', 'session_key', 'driver_number', 'timestamp_utc']) }} as interval_id,
        meeting_key,
        session_key,
        driver_number,
        timestamp_utc,
        to_date(timestamp_utc) as event_date,
        gap_to_leader_raw,
        {{ to_interval_seconds('gap_to_leader_raw') }} as gap_to_leader_seconds,
        interval_to_ahead_raw,
        {{ to_interval_seconds('interval_to_ahead_raw') }} as interval_to_ahead_seconds,
        current_timestamp()::timestamp_ntz as dbt_loaded_at
    from source
)

select *
from renamed
qualify row_number() over (
    partition by interval_id
    order by timestamp_utc desc
) = 1
