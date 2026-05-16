{% test accepted_range(model, column_name, min_value, max_value) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}

{% test accepted_drs_state(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not in (0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15)
{% endtest %}

{% test timestamp_not_in_future(model, column_name, tolerance_hours=2) %}
select *
from {{ model }}
where {{ column_name }} > dateadd(hour, {{ tolerance_hours }}, current_timestamp())
{% endtest %}

{% test non_negative(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} < 0
{% endtest %}
