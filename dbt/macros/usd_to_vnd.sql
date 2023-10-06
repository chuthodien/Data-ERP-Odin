{% macro usd_to_vnd(bill_rate) %}
    ({{bill_rate}}*24837)
{% endmacro %}