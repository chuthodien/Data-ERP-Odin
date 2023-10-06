
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

{{ dbt_date.get_base_dates(start_date="2014-01-01", end_date=last_date_nice, datepart="month") }}