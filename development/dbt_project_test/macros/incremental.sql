{% macro incremental_timestamp_predicate(source_column, target_column=None) -%}
    {%- set target_col = target_column or source_column -%}
    {%- if is_incremental() -%}
        and {{ source_column }} >= (
            select dateadd(
                day,
                -{{ var('incremental_lookback_days', 3) }},
                coalesce(max({{ target_col }}), '1900-01-01'::timestamp_ntz)
            )
            from {{ this }}
        )
    {%- endif -%}
{%- endmacro %}
