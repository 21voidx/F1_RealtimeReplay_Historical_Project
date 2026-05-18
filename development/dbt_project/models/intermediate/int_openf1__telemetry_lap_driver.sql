{{ config(materialized='table', cluster_by=['session_key', 'driver_number', 'lap_number']) }}

select
    l.lap_id,
    l.meeting_key,
    l.session_key,
    l.driver_number,
    l.lap_number,
    l.lap_start_at,
    l.lap_end_at,
    count(c.car_data_id) as telemetry_sample_count,
    avg(c.speed) as avg_speed,
    max(c.speed) as max_speed,
    avg(c.throttle) as avg_throttle,
    max(c.throttle) as max_throttle,
    avg(c.rpm) as avg_rpm,
    max(c.rpm) as max_rpm,
    avg(c.n_gear) as avg_gear,
    sum(c.is_braking) as brake_sample_count,
    sum(c.is_full_throttle) as full_throttle_sample_count,
    sum(c.is_drs_active) as drs_active_sample_count,
    sum(c.is_drs_detected) as drs_detected_sample_count,
    iff(count(c.car_data_id) = 0, null, sum(c.is_braking) / count(c.car_data_id)) as brake_usage_pct,
    iff(count(c.car_data_id) = 0, null, sum(c.is_full_throttle) / count(c.car_data_id)) as full_throttle_pct,
    iff(count(c.car_data_id) = 0, null, sum(c.is_drs_active) / count(c.car_data_id)) as drs_active_pct,
    iff(count(c.car_data_id) = 0, null, sum(c.is_drs_detected) / count(c.car_data_id)) as drs_detected_pct
from {{ ref('int_openf1__lap_enriched') }} l
left join {{ ref('stg_openf1__car_data') }} c
    on l.session_key = c.session_key
   and l.driver_number = c.driver_number
   and c.telemetry_at >= l.lap_start_at
   and c.telemetry_at < l.lap_end_at
group by 1, 2, 3, 4, 5, 6, 7
