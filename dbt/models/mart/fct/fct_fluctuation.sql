{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH fct_employee_fluctuation AS (
    SELECT
        *
    FROM
        {{ ref('fct_employee_fluctuation') }}
),
total_fluctuation AS (
    SELECT
        date_at,
        SUM(onboard_value) AS onboard_count,
        SUM(offboard_value) AS offboard_count,
        SUM(onboard_value) - SUM(offboard_value) AS fluctuation_count
    FROM
        fct_employee_fluctuation
    GROUP BY date_at
),
aggregate_fluctuation AS (
    SELECT 
        date_at
        ,onboard_count
        ,offboard_count
        ,fluctuation_count
        ,sum(fluctuation_count) over (order by date_at asc) AS total_count
    FROM total_fluctuation
    ORDER  BY date_at
)
SELECT
    *
FROM
    aggregate_fluctuation
