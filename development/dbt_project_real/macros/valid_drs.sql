{% test valid_drs(model, column_name) %}
with validation as (
    select
        {{ column_name }} as drs_value
    from {{ model }}
    where {{ column_name }} is not null
),
validation_errors as (
    select
        drs_value
    from validation
    where drs_value not in (0, 1, 2, 3, 8, 9, 10,11, 12, 13,14, 15)  -- Valid DRS states from OpenF1 documentation
)

select *
from validation_errors
{% endtest %}