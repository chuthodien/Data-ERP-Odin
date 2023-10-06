{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH stg_employee AS (
    SELECT
        *
    FROM 
        {{ ref( 'stg_employee' ) }}
),
stg_employee_workinghistories AS (
    SELECT 
        *
    FROM 
        {{ ref( 'stg_employee_workinghistories' ) }}
),
stg_branch AS (
    SELECT
        *
    FROM
        {{ ref('stg_branch') }}
),
employee_types AS (
    SELECT
        *
    FROM
        {{ ref('employee_types') }}
),
stg_months AS (
    SELECT
        *
    FROM
        {{ ref('stg_months') }}
),
employee_working_histories AS (
    SELECT 
        e.hrm_user_id,
        e.sex,
        e.email,
        e.birthday,
        e.branch,
        b.branch_name,
        e.fullname,
        e.employee_type,
        et.employee_type_code,
        e.start_working_time,
        e.is_quit,
        m.date_month,
        (
            CASE
                WHEN m.date_month = date_trunc('month', equit.date_at) 
                THEN equit.status
                WHEN date_trunc('month', ematernity.date_at) <= m.date_month AND m.date_month <= date_trunc('month', ematernity.back_date)
                THEN ematernity.status
                WHEN date_trunc('month', epause.date_at) <= m.date_month AND m.date_month <= date_trunc('month', epause.back_date)
                THEN epause.status
                ELSE ework.status
            END
        ) as status
    FROM stg_employee e
    JOIN employee_types et ON e.employee_type = et.employee_type_id
    JOIN stg_branch b ON e.branch = b.branch_id
    LEFT JOIN stg_employee_workinghistories equit ON equit.employee_id = e.hrm_user_id AND equit.status = 3
    LEFT JOIN stg_employee_workinghistories ework ON ework.employee_id = e.hrm_user_id AND ework.status = 1
    LEFT JOIN stg_employee_workinghistories epause ON epause.employee_id = e.hrm_user_id AND epause.status = 2
    LEFT JOIN stg_employee_workinghistories ematernity ON ematernity.employee_id = e.hrm_user_id AND ematernity.status = 4
    JOIN stg_months m ON date_trunc('month',ework.date_at) <= m.date_month AND (m.date_month <= date_trunc('month',equit.date_at) OR equit.date_at is null)
)
SELECT 
    *
FROM 
    employee_working_histories