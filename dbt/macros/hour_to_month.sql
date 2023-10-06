{% macro hour_to_month(working_time, working_days_month) %}
    ({{working_time}} :: FLOAT / 8 / {{ working_days_month }})
{% endmacro %}