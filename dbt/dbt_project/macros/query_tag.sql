{% macro set_query_tag() -%}
  {% set new_query_tag = var('query_tag_prefix', 'openf1_dbt') ~ ':' ~ target.name ~ ':' ~ model.name %}
  {% set original_query_tag = get_current_query_tag() %}
  {% do run_query("alter session set query_tag = '" ~ new_query_tag ~ "'") %}
  {{ return(original_query_tag) }}
{%- endmacro %}
