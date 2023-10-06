{% macro concat_timestamp(
        date_col,
        time_col,
        fmt = "YYYY-MM-DD HH24:MI:SS"
    ) %}
    TO_TIMESTAMP(
        CONCAT(SUBSTRING({{ date_col }}, 0, 10), ' ', COALESCE(NULLIF({{ time_col }}, ''), '00:00'), ':00'),
        '{{ fmt }}'
    )
{% endmacro %}
