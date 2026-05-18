{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='interval_id',
    cluster_by=['session_key', 'driver_number', 'interval_at'],
    on_schema_change='sync_all_columns',
    tags=['large', 'intervals']
) }}

with source as (
    select
        meeting_key::number as meeting_key,
        session_key::number as session_key,
        driver_number::number as driver_number,
        date::timestamp_ntz as interval_at,
        {{ nullif_text('gap_to_leader') }} as gap_to_leader_raw,
        {{ nullif_text('interval') }} as interval_to_ahead_raw
    from {{ source('openf1_raw', 'raw_intervals') }}
    where session_key is not null
      and driver_number is not null
      and date is not null
      {% if is_incremental() %}
      and date >= dateadd(day, -{{ var('incremental_lookback_days', 3) }}, coalesce((select max(interval_at) from {{ this }}), '1900-01-01'::timestamp_ntz))
      {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['session_key', 'driver_number', 'interval_at']) }} as interval_id,
        meeting_key,
        session_key,
        driver_number,
        interval_at,
        gap_to_leader_raw,
        interval_to_ahead_raw,
        {{ parse_gap_seconds('gap_to_leader_raw') }} as gap_to_leader_seconds,
        {{ parse_gap_seconds('interval_to_ahead_raw') }} as interval_to_ahead_seconds,
        {{ is_lapped_gap('gap_to_leader_raw') }} as is_lapped_to_leader,
        {{ is_lapped_gap('interval_to_ahead_raw') }} as is_lapped_to_ahead,
        current_timestamp()::timestamp_ntz as dbt_loaded_at
    from source
)

select *
from renamed
qualify row_number() over (
    partition by interval_id
    order by interval_at desc
) = 1
