{% test rpm_range(model, column_name) %}
with validation as (
    select
        {{ column_name }} as rpm_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        rpm_value
    from validation
    where rpm_value < 0 or rpm_value > 15000
)
select *
from validation_errors
{% endtest %}