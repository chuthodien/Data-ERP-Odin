{% macro calc_start_working_time() %}
    (CASE 
        WHEN startworkingtime IS NOT NULL AND startworkingtime > '2021-01-01'
        THEN {{ to_timestamp("startworkingtime") }}
        ELSE {{ to_timestamp("creationtime") }}
    END)
{% endmacro %}
