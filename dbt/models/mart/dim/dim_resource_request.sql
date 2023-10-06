{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('stg_resource_requests') }}
),
dim_project AS (

    SELECT
        *
    FROM
        {{ ref('dim_project') }}
)
SELECT
    dp.*,
    s."quantity",
    s."request_name",
    s."dm_note",
    s."pm_note",
    s."request_status",
    s."request_done_time",
    s."request_end_time",
    s."skill_name",
    s."request_start_time"
FROM
    source s
LEFT JOIN dim_project dp
ON dp.prj_project_id = s.prj_project_id

