{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH project_contribution AS (
    SELECT 
        hrm_user_id,
        SUM(user_project_bonus) AS user_project_bonus,
        SUM(user_sqrt_project_bonus) AS user_sqrt_project_bonus
    FROM 
        {{ref('fct_project_contribution')}}
    WHERE "year" = 2023
    GROUP BY hrm_user_id
),
pm_bonus_factor AS (
    SELECT
        *
    FROM
        {{ref('pm_bonus_factor')}}
),
billed_account_by_pm AS (
    SELECT
        pm_user_id,
        SUM({{pm_bonus("total_billed", "pm_bonus_factor")}}) AS pm_bonus
    FROM    
        {{ref('fct_billed_account_by_pm')}} source
    LEFT JOIN pm_bonus_factor ON source.pm_user_id IS NOT NULL
    WHERE "year" = 2023
    GROUP BY pm_user_id
),
review_interns AS (
    SELECT 
        reviewer_id,
        COUNT(internship_id) AS review_interns
    FROM 
        {{ref('dim_review_interns')}}
    WHERE "year" = 2023
    GROUP BY reviewer_id
),
checkpoint_rank AS (
    SELECT 
        *
    FROM 
        {{ref('dim_checkpoint_rank')}}
),
employee AS (
    SELECT 
        *
    FROM 
        {{ref('dim_employee')}}
),
monthly_salary AS (
    SELECT
        MAX(to_salary) as to_salary, employee_id
    FROM
        {{ref('dim_user_monthly_salary')}}
    GROUP BY employee_id
),
data_mapped AS (
    SELECT 
        employee.hrm_user_id,
        employee.email,
        employee.fullname,
        employee.employee_level_name,
        employee.employee_status_code,
        monthly_salary.to_salary as salary,
        COALESCE(source.user_project_bonus, 0) AS user_project_bonus,
        COALESCE(source.user_sqrt_project_bonus, 0) AS user_sqrt_project_bonus,
        COALESCE(bpm.pm_bonus, 0) AS pm_bonus,
        COALESCE(ri.review_interns, 0) AS review_interns,
        COALESCE(cr.contest_rank, 6) AS contest_rank,
        COALESCE(cr.checkpoint_rank, 0) AS checkpoint_rank 
    FROM project_contribution source 
    INNER JOIN employee ON source.hrm_user_id = employee.hrm_user_id
    INNER JOIN monthly_salary ON source.hrm_user_id = monthly_salary.employee_id
    LEFT JOIN billed_account_by_pm bpm ON bpm.pm_user_id = employee.prj_user_id
    LEFT JOIN review_interns ri ON ri.reviewer_id = employee.tsh_user_id
    LEFT JOIN checkpoint_rank cr ON cr.employee_email = employee.email 
)
SELECT 
    *,
    CASE WHEN salary > 0 THEN user_project_bonus/salary ELSE 0 END AS bonus_factor,
    CASE WHEN salary > 0 THEN user_sqrt_project_bonus/salary ELSE 0 END AS sqrt_bonus_fator
FROM 
    data_mapped
