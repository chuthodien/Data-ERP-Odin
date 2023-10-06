{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH monthly_salary AS (
    SELECT 
        *
    FROM 
        {{ref('dim_user_monthly_salary')}}
),
monthly_working_days AS (
    SELECT  
        *
    FROM 
        {{ref('dim_user_monthly_working_days')}}
    WHERE working_days > 0
),
monthly_benefits AS (
    SELECT 
        *
    FROM
        {{ref('dim_user_monthly_benefits')}}
),
dim_month AS (
    SELECT 
        *
    FROM 
        {{ref('dim_month')}}
),
dim_employee AS (
    SELECT 
        *
    FROM
        {{ref('dim_employee')}}
),
data_mapped AS (
    SELECT
        e.hrm_user_id,
        e.email,
        e.fullname,
        COALESCE(bm.total_benefit, 0) AS total_benefit,
        COALESCE(benefit_numbers, 0) AS benefit_numbers,
        COALESCE({{calc_salary("s.to_salary", "wd.working_days", "m.working_days_of_month")}}, 0) AS total_salary,
        COALESCE(s.time_at_month, bm.time_at_month) AS time_at_month,
        s.employee_level_code,
        s.branch_name,
        COALESCE(wd.working_days, 0) AS working_days,
        s.to_salary
    FROM 
    (
        SELECT
            b.employee_id,
            b.time_at_month,
            SUM({{calc_benefit("b.benefit_type", "b.money", "wd.working_days", "m.working_days_of_month")}}) AS total_benefit,
            COUNT(b.benefit_id) AS benefit_numbers
        FROM monthly_benefits b
        JOIN dim_employee e ON b.employee_id = e.hrm_user_id
        LEFT JOIN monthly_working_days wd ON b.time_at_month = wd.time_at_month AND e.tsh_user_id = wd.tsh_user_id
        LEFT JOIN dim_month m ON b.time_at_month = m.date_month
        GROUP BY b.employee_id, b.time_at_month
        
    ) AS bm
    RIGHT JOIN monthly_salary s ON bm.time_at_month = s.time_at_month AND bm.employee_id = s.employee_id
    INNER JOIN dim_employee e ON s.employee_id = e.hrm_user_id
    LEFT JOIN monthly_working_days wd ON wd.time_at_month = s.time_at_month AND wd.tsh_user_id = e.tsh_user_id
    LEFT JOIN dim_month m ON m.date_month = s.time_at_month 
    ORDER BY time_at_month, branch_name, employee_level_code
)
SELECT 
	* 
FROM 
    data_mapped