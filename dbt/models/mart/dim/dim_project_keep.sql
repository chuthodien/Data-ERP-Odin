{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_projects AS (

    SELECT
        *
    FROM
        {{ ref(
            'stg_project_keep'
        ) }}
)
SELECT
        "tsh_project_id",
        "tsh_project_code",
        "project_name",
        "project_note",
        "tsh_project_status",
        "end_end",
        "start_time",
        "tsh_customer_id",
        "tsh_project_type",
        "is_general",
        "is_deleted",
        "creation_time",
        "deletion_time",
        "last_modification_time"
FROM
    stg_projects sp
