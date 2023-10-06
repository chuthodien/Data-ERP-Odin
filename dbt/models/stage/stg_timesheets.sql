{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_tsh_mytimesheets'
        ) }}
), 
stg_employee AS (
    SELECT
        *
    FROM
        {{ ref(
            'stg_employee'
        ) }}
),
base_tsh_taskprojects AS (
    SELECT 
        *
    FROM {{ ref('base_tsh_taskprojects') }} 

),
data_mapped AS (
    SELECT 
        {{add_date_at('time_at')}},
        s.*,
        btt."tsh_task_id",
        btt."billable",
        btt."tsh_project_id",
        se.email AS "employee_email"
    FROM source s
    LEFT JOIN base_tsh_taskprojects btt
        ON s.tsh_projecttask_id = btt.tsh_taskprojects_id
    LEFT JOIN stg_employee se
        ON s.tsh_user_id = se.tsh_user_id
    WHERE se.email IS NOT NULL
)
SELECT
    *
FROM
    data_mapped
