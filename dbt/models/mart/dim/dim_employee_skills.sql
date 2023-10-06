{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('stg_employee_skills') }}
)
SELECT
    s.*
FROM
    source s


