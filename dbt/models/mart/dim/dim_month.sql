{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_date AS (
    SELECT
        *
    FROM
        {{ ref('stg_dates') }}
),
stg_months AS (
    SELECT
        *
    FROM
        {{ ref('stg_months') }}
)
SELECT
    dm.*,
    dd.*,
    {{ working_days_of_month("dd.month_start_date", "dd.month_end_date") }} AS working_days_of_month
FROM
    stg_months dm
LEFT JOIN dim_date dd
    ON dd.date_day = dm.date_month
