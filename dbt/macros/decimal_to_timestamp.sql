{% macro decimal_to_timestamp(
        column_name
    ) %}
    (
        TO_TIMESTAMP(
            (
                REPLACE(createdTimestamp::VARCHAR, 'E12', '')::NUMERIC
                * POWER(10, 9)
            )
        )
    )
{% endmacro %}
