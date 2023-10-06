{% macro to_timestamp(
        column_name,
        fmt = "YYYY-MM-DD HH24:MI:SS"
    ) %}
    TO_TIMESTAMP({{ column_name }}, '{{ fmt }}')
{% endmacro %}
