{{ config(
    schema = 'mart',
    materialized = "table"
) }}
SELECT
    *
FROM
    {{ ref('dim_employee_project') }}