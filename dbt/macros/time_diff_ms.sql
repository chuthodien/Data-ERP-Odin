{% macro time_diff_ms(
        timestamp_A,
        timestamp_B
    ) %} (EXTRACT(epoch FROM ({{ timestamp_B }} - {{ timestamp_A }} ) ) * 1000) {% endmacro %}
