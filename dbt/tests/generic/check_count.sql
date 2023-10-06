{% test check_count(model, column_name, field, to) %}
{{ config(store_failures = true) }}
with parent as (
    select
    count(*) as count_parent
    from {{ to }}
),

child as (
    select
        count(*) as count_child
    from {{ model }}

)
select *
from child c,
 parent p 
where c.count_child <> p.count_parent
{% endtest %}