{% macro trim_qoutes_col(
        column_name
    ) %}
    (REPLACE({{column_name}}::VARCHAR, '"', '')) AS {{column_name}}
{% endmacro %}
