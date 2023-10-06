{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH project_users AS(

    SELECT
        *
    FROM
        {{ ref('stg_project_projectusers') }}
),
projects AS (
    SELECT
        *
    FROM
        {{ ref('stg_projects') }}
    WHERE
        is_deleted IS FALSE
        AND project_status = 1
),
project_user_status AS (
    SELECT
        *
    FROM
        {{ ref('project_user_status') }}
),
data_mapped AS(
    SELECT
        pu.project_user_id,
        pu.project_user_note AS note,
        pu.is_temp,
        pu.project_user_status,
        pus.status_code AS project_user_status_code,
        pu.prj_user_id AS user_id,
        pu.project_id,
        pu.is_expense,
        pu.user_start_time,
        pu.project_role,
        pu.is_future_active,
        pu.resource_request_id,
        pu.allocate_percentage,
        p.project_code,
        p.project_status,
        p.is_deleted AS project_is_deleted,
        p.start_time AS project_start_time,
        p.end_time AS project_end_time,
        p.project_type,
        p.pm_email,
        employee_project_last_modification_time,
        (
            CASE
                WHEN project_user_status = 0 THEN TRUE
                WHEN project_user_status = 1
                AND EXTRACT(
                    epoch
                    FROM
                        now() - employee_project_last_modification_time
                ) / (
                    3600 * 24
                ) <= 30 * 6 THEN TRUE
                ELSE FALSE
            END
        ) :: BOOLEAN AS active_last_six_month
    FROM
        project_users pu
        INNER JOIN projects p
        ON pu.project_id = p.prj_project_id
        LEFT JOIN project_user_status pus
        ON pu.project_user_status = pus.status_id
)
SELECT
    *
FROM
    data_mapped
