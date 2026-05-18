{% test throttle_range(model, column_name) %}
with validation as (
    select
        {{ column_name }} as throttle_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        throttle_value
    from validation
    where throttle_value < 0 or throttle_value > 110
)
select *
from validation_errors
{% endtest %}
