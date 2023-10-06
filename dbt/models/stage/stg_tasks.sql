{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_tsh_tasks'
        ) }}
),
data_mapped AS (
    SELECT
        "tsh_task_id",
        "task_name",
        "task_type",
        "is_deleted",
        "creation_time",
        "deletion_time",
        "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_mapped
