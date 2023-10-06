{{ config(
    schema = 'mart',
    materialized = "table"
) }}
WITH employee AS (
    SELECT * FROM {{ref('stg_employee')}}
),
member_project AS (
    SELECT 
        tsh_user_id,
        time_at,
        SUM(working_time)/60/60/1000 AS working_hour,
        date_trunc('month', time_at) AS start_month_date
    FROM {{ref('stg_member_project_tsh')}}
    GROUP BY tsh_user_id, time_at
),
monthly_salary AS (
    SELECT * FROM {{ref('stg_user_monthly_salary')}}
),
dim_month AS (
    SELECT * FROM {{ref('dim_month')}}
),
dim_branch AS (
    SELECT * FROM {{ref('dim_branch')}}
),
employee_level AS (
    SELECT * FROM {{ref('stg_employee_levels')}}
),
data_mapped AS (
    SELECT DISTINCT
        em.hrm_user_id,
        em.email,
        em.fullname,
        SUM(wt.working_hour) OVER (PARTITION BY wt.tsh_user_id, wt.start_month_date )/8 ::FLOAT AS "working_day",
        s.user_level,
        el.level_name,
        em.status,
	    s.time_at_month,
        dm.working_days_of_month,
        s.branch,
        db.branch_name,
        s.to_salary
    FROM employee em
    JOIN member_project wt ON wt.tsh_user_id = em.tsh_user_id
    JOIN monthly_salary s ON s.employee_id = em.hrm_user_id AND s.time_at_month = wt.start_month_date
    JOIN dim_branch db ON s.branch = db.branch_id
    JOIN employee_level el ON s.user_level= el.level_id
    JOIN dim_month dm ON dm.month_start_date=s.time_at_month
    ORDER BY em.email,s.time_at_month desc
)
SELECT 
    *,
    to_salary/(working_days_of_month + 1)*working_day AS "last_salary"
FROM data_mapped
