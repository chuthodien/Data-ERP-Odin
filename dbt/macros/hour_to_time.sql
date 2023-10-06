{% macro hour_to_time(
        time_col
    ) %}
    (to_char(to_timestamp((COALESCE({{time_col}}, 0) * 60), 'MI:SS'))
{% endmacro %}
