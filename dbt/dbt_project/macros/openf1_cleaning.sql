{% macro to_interval_seconds(expr) -%}
    case
        when {{ expr }} is null then null
        when lower(trim({{ expr }}::string)) in ('', 'none', 'null', 'nan') then null
        when regexp_like(trim({{ expr }}::string), '^[+-]?[0-9]+(\\.[0-9]+)?$') then try_to_double({{ expr }})
        when regexp_like(trim({{ expr }}::string), '^[+-]?[0-9]+:[0-9]{2}(\\.[0-9]+)?$') then
            try_to_double(split_part({{ expr }}::string, ':', 1)) * 60
            + try_to_double(split_part({{ expr }}::string, ':', 2))
        when upper(trim({{ expr }}::string)) like '%L%' then null
        else null
    end
{%- endmacro %}

{% macro colour_to_hex(expr) -%}
    case
        when {{ expr }} is null then null
        when trim({{ expr }}::string) = '' then null
        when left(trim({{ expr }}::string), 1) = '#' then upper(trim({{ expr }}::string))
        else '#' || upper(trim({{ expr }}::string))
    end
{%- endmacro %}

{% macro nullif_text(expr) -%}
    nullif(nullif(nullif(trim({{ expr }}::string), ''), 'None'), 'null')
{%- endmacro %}
