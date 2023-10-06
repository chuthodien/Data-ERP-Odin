{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH timesheet AS (

    SELECT
        *
    FROM
        {{ ref('stg_timesheets') }}
),
external_taskprojects_id AS(
    SELECT
        tsh_project_task_id,
        tsh_project_id
    FROM
        {{ ref ('dim_bill_project_tasks') }}
),
data_mapped AS(
    SELECT
        tsh_mytimesheet_id AS bill_workingtime_id,
        time_at,
        CAST({{ dbt_utils.date_trunc('month', 'time_at') }} AS TIMESTAMP) AS month_start_date,
        tsh_user_id AS user_id,
        "status",
        is_charge,
        type_of_work,
        CAST (
            working_time AS FLOAT
        ) / (CAST(60 * 60 * 1000 AS FLOAT)) AS working_hour,
        tsh_projecttask_id AS project_task_id,
        tsh_project_id AS project_id
    FROM
        timesheet
    WHERE
        is_deleted IS FALSE
        AND timesheet.tsh_projecttask_id IN (
            SELECT
                tsh_project_task_id
            FROM
                external_taskprojects_id
        )
        AND working_time > 0
),
tsh_projects AS (
    SELECT
        *
    FROM
        {{ ref('stg_tsh_projects') }}
),
mapped_project_name AS(
    SELECT
        data_mapped.*,
        tsh_projects.tsh_project_code AS project_code
    FROM
        data_mapped
        JOIN tsh_projects
        ON data_mapped.project_id = tsh_projects.tsh_project_id
)
SELECT
    *
FROM
    mapped_project_name
