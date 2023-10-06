{% macro add_date_at(date_col, date_at_col = 'date_at') %}
    ({{dbt_utils.date_trunc('day', date_col)}})::TIMESTAMP AS {{date_at_col}}
{% endmacro %}
