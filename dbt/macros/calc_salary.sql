{% macro calc_salary(
        salary,
        user_working_days,
        working_days_of_month
    ) %}
    ( {{salary}}::FLOAT / ({{working_days_of_month}} + 1) * {{user_working_days}})
{% endmacro %}