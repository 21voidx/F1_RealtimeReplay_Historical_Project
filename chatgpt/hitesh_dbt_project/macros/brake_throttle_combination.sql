-- Add this to macros/f1_car_data_tests.sql

{% test brake_throttle_combination(model) %}
with high_values as (
    select
        date,
        brake,
        throttle,
        speed,
        rpm
    from {{ model }}
    where brake > 105  -- Allowing for some sensor overflow
      and throttle > 105  -- Allowing for some sensor overflow
      -- Only flagging when both brake and throttle are extremely high
      -- and when we're not at very low speeds (could be valid during starts/pit exits)
      and speed > 50
      -- Excluding engine warmup/testing scenarios
      and rpm > 5000
),

-- Look for sustained periods of extreme values
sustained_issues as (
    select *,
        lead(date) over (order by date) as next_reading,
        lag(date) over (order by date) as prev_reading
    from high_values
)

-- Return only the problematic readings that persist for multiple samples
select
    date,
    brake,
    throttle,
    speed,
    rpm
from sustained_issues
where datediff('millisecond', prev_reading, date) < 500  -- Looking for sustained issues
   or datediff('millisecond', date, next_reading) < 500
{% endtest %}