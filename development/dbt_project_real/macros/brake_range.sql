{% test brake_range(model, column_name) %}
with validation as (
    select
        {{ column_name }} as brake_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        brake_value
    from validation
    where brake_value < 0 or brake_value > 100 -- accounting for tests in practices
)
select *
from validation_errors
{% endtest %}
