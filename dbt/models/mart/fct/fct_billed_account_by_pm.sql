{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH project AS (
    SELECT
        *
    FROM {{ref('dim_project')}}
),
dim_month AS (
    SELECT
        *
    FROM {{ref('dim_month')}}
),
name_pm AS (
    SELECT
        *
    FROM {{ref('dim_employee')}}
),
source AS (
    SELECT 
        *,
        (
            CASE
                WHEN currency_id = 2 THEN {{ usd_to_vnd("bill_rate") }}
                WHEN currency_id = 3 THEN bill_rate
                WHEN currency_id = 4 THEN {{ aud_to_vnd("bill_rate") }}
            END
        ) AS bill_rate_vnd
    FROM {{ref('dim_project_bill')}}
),
bill_account_pm AS (
    SELECT
        p.pm_user_id,
        p.pm_email,
        p.project_id,
        COUNT(p.prj_user_id) AS "account",
        SUM({{project_income("p.bill_rate_vnd", "p.working_time", "p.charge_type", "dm.working_days_of_month") }}) AS "total_billed",
        p.year,
        p.month
    FROM source p 
    JOIN dim_month dm 
    ON p.month = dm.month_of_year AND p.year = dm.year_number
    GROUP BY
        p.pm_email,
        p.pm_user_id,
        p.project_id,
        p.year,
        p.month
)
    SELECT
        pm.*,
	na.fullname,
	pr.project_code
    FROM bill_account_pm pm
    JOIN project pr
    ON pm.project_id = pr.prj_project_id
    JOIN name_pm na 
    ON pm.pm_email = na.email
    