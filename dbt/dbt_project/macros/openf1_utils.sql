{% macro nullif_text(column_name) -%}
    nullif(trim({{ column_name }}::string), '')
{%- endmacro %}

{% macro bool_to_int(expression) -%}
    case when {{ expression }} then 1 else 0 end
{%- endmacro %}

{% macro non_negative_or_null(column_name) -%}
    ({{ column_name }} is null or {{ column_name }} >= 0)
{%- endmacro %}

{% macro parse_gap_seconds(column_name) -%}
    case
        when {{ column_name }} is null then null
        when regexp_like(upper(trim({{ column_name }}::string)), '^\\+[0-9]+\\s*LAP') then null
        else try_to_double({{ column_name }}::string)
    end
{%- endmacro %}

{% macro is_lapped_gap(column_name) -%}
    case
        when {{ column_name }} is null then false
        when regexp_like(upper(trim({{ column_name }}::string)), '^\\+[0-9]+\\s*LAP') then true
        else false
    end
{%- endmacro %}

{% macro is_drs_active(column_name) -%}
    ({{ column_name }} in (10, 12, 14))
{%- endmacro %}

{% macro is_drs_detected(column_name) -%}
    ({{ column_name }} in (8, 10, 12, 14))
{%- endmacro %}

{% macro lap_window_end_expr() -%}
    coalesce(next_lap_start_at, dateadd(minute, 5, lap_start_at))
{%- endmacro %}
