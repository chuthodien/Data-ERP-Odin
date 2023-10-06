{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH project_users AS(

    SELECT
        *
    FROM
        {{ ref('dim_project_projectusers') }}
    WHERE
        project_user_status = 0
        AND project_is_deleted IS FALSE
        AND project_status = 1
        AND allocate_percentage > 0
),
users AS(
    SELECT
        *
    FROM
        {{ ref('dim_project_users') }}
),
data_mapped AS(
    SELECT
        users.prj_user_id,
        users.lastname,
        users.surname,
        users.star_rate,
        users.is_active,
        users.pool_note,
        users.usercode,
        users.email,
        users.branch_code,
        users.job_type_code,
        users.employee_type_code,
        users.employee_level_name,
        (
            CASE
                WHEN users.prj_user_id IN (
                    SELECT
                        DISTINCT user_id
                    FROM
                        project_users
                    WHERE
                        is_temp IS FALSE
                ) THEN 'Official'
                WHEN users.prj_user_id IN(
                    SELECT
                        DISTINCT user_id
                    FROM
                        project_users
                    WHERE
                        is_temp IS TRUE
                ) THEN 'Temp'
                ELSE 'Pool'
            END
        ) :: VARCHAR work_status
    FROM
        users
)
SELECT
    *
FROM
    data_mapped
