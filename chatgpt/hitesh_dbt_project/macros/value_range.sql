{% test value_range(model, column_name, min_value, max_value) %}
with validation as (
    select
        {{ column_name }} as field_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        field_value
    from validation
    where field_value < {{ min_value }} or field_value > {{ max_value }}
)
select count(*)
from validation_errors
{% endtest %}