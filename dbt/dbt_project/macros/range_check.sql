{% test range_check(model, column_name, min_value, max_value) %}

with validation as (
    select
        {{ column_name }} as field_value
    from {{ model }}
    where {{ column_name }} is not null  -- Only test non-null values
),

validation_errors as (
    select
        field_value
    from validation
    where field_value < {{ min_value }} or field_value > {{ max_value }}
)

select *
from validation_errors

{% endtest %}