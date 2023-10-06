{% macro add_start_of_month(
        date_col,
        date_at_col = 'start_of_month'
    ) %}
    ({{ dbt_utils.date_trunc('month', date_col) }}) :: TIMESTAMP AS {{ date_at_col }}
{% endmacro %}
