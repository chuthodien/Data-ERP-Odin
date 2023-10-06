{% macro pm_bonus(
        total_billed,
        factor
    ) %}
    ({{total_billed}} * {{factor}})
{% endmacro %}
