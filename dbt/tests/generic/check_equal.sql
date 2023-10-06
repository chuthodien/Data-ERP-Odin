{% test check_equal(model, column_name, field, to) %}
{{ config(store_failures = true) }}
with parent as (
    select
    {{ field }} as id
    from {{ to }}
),

child as (
    select
        {{ column_name }} as id
    from {{ model }}

)

select *
from child c
where id not in (select id from parent)

{% endtest %}