{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH source AS (
    SELECT 
        *
    FROM 
        {{ref('stg_user_monthly_salary')}}
)
SELECT 
    *
FROM 
    source
