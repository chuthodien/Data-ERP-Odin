{% macro aud_to_vnd(bill_rate) %}
    ({{bill_rate}}*16430)
{% endmacro %}