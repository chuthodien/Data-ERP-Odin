{% test unique_active_record(model, column_name) %}
SELECT
    {{column_name}} AS unique_field,
    COUNT(*) AS n_records
FROM
    {{ model }}
WHERE
    {{model}}.is_deleted IS FALSE
GROUP BY
    {{column_name}}
HAVING
    COUNT(*) > 1 
{% endtest %}
