{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS(

    SELECT
        *
    FROM
        {{ ref('base_tsh_taskprojects') }}
),
data_type_rename_conversion AS(
    SELECT
        tsh_taskprojects_id AS tsh_project_task_id,
        tsh_task_id,
        tsh_project_id,
        is_deleted,
        creation_time,
        deletion_time,
        last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
