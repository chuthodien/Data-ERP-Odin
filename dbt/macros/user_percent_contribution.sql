{% macro user_percent_contribution(
        working_time,
        salary,
        project_type,
        project_name,
        user_level,
        retro_point
    ) %}
    
    case
        when {{ project_type }} = 5 and {{ user_level }} < 5
        then 0
        when {{ project_name }} like 'Noname' and {{ user_level }} < 5
        then 0
        else {{ working_time }} * {{salary}} * {{retro_point}}
    end
{% endmacro %}
