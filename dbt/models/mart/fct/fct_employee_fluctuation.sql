{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_employee_onboard AS (
    SELECT
        employee_email,
        date_at,
        1 :: INT AS fluctuation,
        1 :: INT AS onboard_value,
        0 :: INT AS offboard_value,
        1 :: INT AS fluctuation_value
    FROM
        {{ ref('dim_employee_onboard') }}
),
dim_employee_offboard AS (
    SELECT
        employee_email,
        date_at,
        0 :: INT AS fluctuation,
        0 :: INT AS onboard_value,
        1 :: INT AS offboard_value,
        -1 :: INT AS fluctuation_value
    FROM
        {{ ref('dim_employee_offboard') }}
)
SELECT
    *
FROM
    dim_employee_onboard
UNION
SELECT
    *
FROM
    dim_employee_offboard
