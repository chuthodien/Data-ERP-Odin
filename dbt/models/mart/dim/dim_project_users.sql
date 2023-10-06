{{ config (
    materialized = 'table',
    schema = 'mart'
) }}

WITH users AS (

    SELECT
        *
    FROM
        {{ ref('stg_project_users') }}
    WHERE
        is_deleted IS FALSE
        AND is_active IS TRUE
),
employee_types AS (
    SELECT
        *
    FROM
        {{ ref('employee_types') }}
),
employee_levels AS (
    SELECT
        *
    FROM
        {{ ref('employee_levels') }}
),
project_branch AS(
    SELECT
        *
    FROM
        {{ ref('stg_project_branchs') }}
),
job_types AS(
    SELECT
        *
    FROM
        {{ ref('job_types') }}
),
data_mapped AS (
    SELECT
        users.*,
        CONCAT(
            surname,
            ' ',
            lastname
        ) AS fullname,
        pb.branch_code,
        jt.job_type_code,
        et.employee_type_code,
        el.employee_level_name
    FROM
        users
        LEFT JOIN employee_types et
        ON users.employee_type = et.employee_type_id
        LEFT JOIN employee_levels el
        ON users.employee_level = el.employee_level_id
        LEFT JOIN project_branch pb
        ON users.branch = pb.prj_branch_id
        LEFT JOIN job_types jt
        ON jt.job_type_id = users.job_type
)
SELECT
    *
FROM
    data_mapped
