{% test speed_range(model, column_name) %}
with validation as (
    select
        {{ column_name }} as speed_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        speed_value
    from validation
    where speed_value < 0 or speed_value > 410
)
select *
from validation_errors
{% endtest %}