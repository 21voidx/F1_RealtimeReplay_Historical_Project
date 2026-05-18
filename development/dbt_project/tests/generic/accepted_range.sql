{% test accepted_range(model, column_name, min_value=None, max_value=None) %}
select *
from {{ model }}
where 1 = 1
{% if min_value is not none %}
  and {{ column_name }} < {{ min_value }}
{% endif %}
{% if max_value is not none %}
  and {{ column_name }} > {{ max_value }}
{% endif %}
{% endtest %}
