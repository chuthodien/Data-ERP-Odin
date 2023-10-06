{% macro working_days_of_month(
    month_start_date,
    month_end_date
) %}
    ( {{ days_of_month(month_start_date) }} - ( {{ first_week_off_days(month_start_date) }} + {{ last_week_off_days(month_end_date) }} + ({{ days_between(days_of_month(month_start_date), month_start_date, month_end_date) }})/7*2 ))
{% endmacro %}

{% macro n_months_away(month_start_date, months) %}
    cast(cast(cast( {{ month_start_date }} ::timestamp at time zone 'UTC' at time zone 'Asia/Bangkok' as timestamp) as date) + ((interval '1 month') * ({{ months }})) as date)
{% endmacro %}

{% macro days_of_month(month_start_date) %}
    {{ dbt_utils.datediff(month_start_date, n_months_away(month_start_date, 1), 'day') }}
{% endmacro %}

{% macro first_week_off_days(month_start_date) %}
    CASE
        WHEN {{ dbt_date.day_of_week(month_start_date, true) }} = 7
        THEN 1
        ELSE 2
    END
{% endmacro %}

{% macro last_week_off_days(month_end_date) %}
    CASE
        WHEN {{ dbt_date.day_of_week(month_end_date, true) }} = 6
        THEN 1
        WHEN {{ dbt_date.day_of_week(month_end_date, true) }} = 7
        THEN 2
        ELSE 0
    END
{% endmacro %}

{% macro days_between(days_of_month, month_start_date, month_end_date) %}
    {{ days_of_month }} - 8 + {{ dbt_date.day_of_week(month_start_date, true) }} - {{ dbt_date.day_of_week(month_end_date, true) }}
{% endmacro %}

