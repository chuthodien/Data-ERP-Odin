{% macro project_income(bill_rate, working_time, charge_type, working_days_month) %}
    ({{bill_rate}} * {{ working_time_converted(working_time, charge_type, working_days_month) }})
{% endmacro %}

{% macro working_time_converted(working_time, charge_type, working_days_month) %}
    case
        when charge_type = 0
        then (working_time)
        when charge_type = 1
        then ({{ day_to_month(working_time, working_days_month) }})
        when charge_type = 2
        then ({{ day_to_hour(working_time) }})
    end
{% endmacro %}
