{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH source AS (
    SELECT
        *
    FROM
        {{ref('stg_benefit_employees')}}
),
stg_user_monthly_salary AS (
    SELECT 
        *
    FROM 
        {{ref('stg_user_monthly_salary')}}
),
data_mapped AS (
    SELECT
        ums.time_at_month,
        source.*
    FROM source
    RIGHT JOIN stg_user_monthly_salary ums
    ON ums.employee_id = source.employee_id AND ums.time_at_month >= source.start_at_month AND (ums.time_at_month < source.end_at_month OR end_at_month IS NULL )
),
data_selection AS (
    SELECT 
        *
    FROM 
        data_mapped
    WHERE hrm_benefitemployee_id IS NOT NULL
)
SELECT 
    *
FROM 
    data_selection
