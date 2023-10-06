{{ config(
    schema = 'stage',
    materialized = "table"
) }}

{% if modules.datetime.date.today().month == 12  %}
{% set last_date = modules.datetime.datetime(modules.datetime.date.today().year, modules.datetime.date.today().month, 31) %}
{% else %}
{% set last_date = modules.datetime.datetime(modules.datetime.date.today().year, modules.datetime.date.today().month + 1, 1) + modules.datetime.timedelta(days=-1) %}
{%endif %}
{% set last_date_nice = last_date.isoformat() %}

{{ dbt_date.get_date_dimension(
    '2014-01-01',
    last_date_nice
) }}
