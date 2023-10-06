{% macro calc_benefit(
        benefit_type,
        benefit_money,
        user_working_days,
        working_days_of_month
    ) %}
    case
        when {{benefit_type}} = 3 then 0
        when {{benefit_type}} = 2 then {{benefit_money}}
        when {{benefit_type}} = 1 then ({{benefit_money}}::FLOAT/({{working_days_of_month}} + 1)*{{user_working_days}})
        else 0
    end
{% endmacro %}