{% macro trim_qoutes(
        col
    ) %}
    (REPLACE({{col}}::VARCHAR, '"', ''))
{% endmacro %}
