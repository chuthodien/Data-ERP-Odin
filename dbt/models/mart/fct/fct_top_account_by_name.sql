{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH project_bill AS (
    SELECT
        *
    FROM {{ref('stg_project_bill')}}
    WHERE is_deleted = false AND is_active :: BOOLEAN = true AND working_time > 0
),
dim_month AS (
    SELECT
        *
    FROM {{ref('dim_month')}}
),
project AS (
    SELECT
        prj_project_id,
        project_code,
        project_name
    FROM {{ref('dim_project')}}
),
email_users AS (
    SELECT
        *
     FROM {{ref('dim_project_users')}}
),
data_mapped AS (
    SELECT
        p.prj_user_id,
        p.project_id,
        e.email,
        p.year,
        p.month,
        p.bill_rate,
        p.working_time,
        pr.project_code,
        pr.project_name,
        {{project_income("p.bill_rate", "p.working_time", "p.charge_type", "dm.working_days_of_month") }} AS "total_bill"
    FROM project_bill p 
    JOIN dim_month dm 
    ON p.month = dm.month_of_year AND p.year = dm.year_number
    JOIN email_users e 
    ON e.prj_user_id = p.prj_user_id
    JOIN project pr 
    ON pr.prj_project_id = p.project_id
)
SELECT
*
FROM data_mapped

