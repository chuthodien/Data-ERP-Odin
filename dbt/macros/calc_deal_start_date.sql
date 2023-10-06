{% macro calc_deal_start_date() %}
    (CASE 
        WHEN dealstartdate IS NULL
        THEN {{ to_timestamp("creationtime") }}
        ELSE {{ to_timestamp("dealstartdate") }}
    END)
{% endmacro %}
