{% macro concat_safe(col1, col2) %}
   (trim(both from COALESCE({{col1}}, '') || ' ' || COALESCE({{col2}}, '')))
{% endmacro %}
