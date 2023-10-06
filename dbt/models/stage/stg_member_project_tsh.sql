{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH tsh_project AS (
    SELECT 
        *
    FROM
        {{ref(
            'base_tsh_projects'
        )}}
    WHERE 
        is_deleted = false
),
tsh_taskproject AS (
    SELECT
        *
    FROM
        {{ref(
            'base_tsh_taskprojects'
        )}}
    WHERE is_deleted = false
),
tsh_task AS (
    SELECT 
        *
    FROM 
        {{ref(
            'base_tsh_tasks'
        )}}
    WHERE is_deleted = false
),
tsh_mytimesheet AS(
    SELECT
        *
    FROM
        {{ref(
            'base_tsh_mytimesheets'
        )}}
    WHERE is_deleted = false
),
data_mapped AS (
    SELECT 
        mtsh.tsh_mytimesheet_id,
        mtsh.time_at,
        mtsh.working_time,
        mtsh.creation_time AS mytimesheet_creation_time,
        mtsh.tsh_user_id,
        t.task_name,
        p.tsh_project_id,
        p.tsh_project_code
    FROM tsh_mytimesheet mtsh
    LEFT JOIN tsh_taskproject tp
    ON mtsh.tsh_projecttask_id = tp.tsh_taskprojects_id
    LEFT JOIN tsh_project p
    ON tp.tsh_project_id = p.tsh_project_id
    LEFT JOIN tsh_task t
    ON tp.tsh_task_id = t.tsh_task_id
    ORDER BY p.tsh_project_id, mtsh.time_at
)
SELECT
    *
FROM
    data_mapped