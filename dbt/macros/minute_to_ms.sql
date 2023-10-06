{% macro minute_to_ms(
        time_col
    ) %}
    (COALESCE({{time_col}}, 0) * 60 * 1000)
{% endmacro %}
