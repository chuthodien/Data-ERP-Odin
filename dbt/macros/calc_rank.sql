{% macro calc_rank(
    max_point, 
    point_col,
    ranks) %}
    ({{ranks}} - CEILING({{point_col}}::FLOAT / {{max_point}}::FLOAT * {{ranks}}) + 1)
{% endmacro %}