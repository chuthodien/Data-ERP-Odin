{% macro ms_to_hour(
        time_col
    ) %}
    (COALESCE(
        {{time_col}},
        0
    ) / 60 / 60 / 1000)
{% endmacro %}