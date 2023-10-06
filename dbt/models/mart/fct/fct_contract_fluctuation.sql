{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH contracts_start AS (
    SELECT
        email AS employee_email,
        contract_start_date AS date_at,
        2 :: INT AS fluctuation,
        1 :: INT AS contract_value,
        1 :: INT AS contract_start_value,
        0 :: INT AS contract_end_value
    FROM
        {{ ref('dim_employee') }}
    WHERE onboard_date IS NOT NULL
),
contract_end AS (
    SELECT
        email AS employee_email,
        offboard_date AS date_at,
        3 :: INT AS fluctuation,
        -1 :: INT AS contract_value,
        0 :: INT AS contract_start_value,
        1 :: INT AS contract_end_value
    FROM
        {{ ref('dim_employee') }}
    WHERE
        offboard_date IS NOT NULL
)
SELECT
    *
FROM
    contracts_start
UNION
SELECT
    *
FROM
    contract_end
