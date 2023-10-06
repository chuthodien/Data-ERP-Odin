{% macro day_to_month(working_time, working_days_month) %}
    ({{working_time}} :: FLOAT / {{ working_days_month }} )
{% endmacro %}