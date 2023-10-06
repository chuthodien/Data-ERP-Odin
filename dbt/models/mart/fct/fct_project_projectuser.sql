{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH users AS (

    SELECT
        *
    FROM
        {{ ref('dim_project_users') }}
),
project_users AS (
    SELECT
        *
    FROM
        {{ ref('dim_project_projectusers') }}
),
data_mapped AS (
    SELECT
        pu.project_user_id,
        pu.note,
        pu.is_temp,
        pu.project_user_status_code,
        pu.user_id,
        pu.project_id,
        pu.is_expense,
        pu.user_start_time,
        pu.project_role,
        pu.is_future_active,
        pu.resource_request_id,
        pu.allocate_percentage,
        pu.project_code,
        pu.project_start_time,
        pu.project_end_time,
        pu.project_type,
        pu.pm_email,
        pu.active_last_six_month,
        u.username,
        u.fullname,
        u.job_type_code,
        u.branch_code,
        u.employee_type_code,
        u.employee_level_name,
        u.is_active,
        pt.project_type_code,
        pt.project_type_name
    FROM
        project_users pu
        INNER JOIN users u
        ON pu.user_id = u.prj_user_id
        LEFT JOIN {{ref('project_types')}} pt
        ON pu.project_type = pt.project_type_id
)
SELECT
    *
FROM
    data_mapped
