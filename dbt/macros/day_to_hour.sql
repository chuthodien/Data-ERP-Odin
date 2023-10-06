{% macro day_to_hour(working_time) %}
    ({{working_time}}*8)
{% endmacro %}