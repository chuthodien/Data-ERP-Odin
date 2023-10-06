
{{ config(
    materialized = 'table',
    schema = 'base'
) }}

WITH base_prj_resourcerequestskills AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_prj_resourcerequestskills'
        ) }}
),
base_prj_resourcerequests AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_prj_resourcerequests'
        ) }}
),
stg_skills AS (
    SELECT
        *
    FROM
        {{ ref(
            'stg_skills'
        ) }}
),
data_selection AS (
    SELECT
        *
    FROM
        base_prj_resourcerequestskills
),
data_type_rename_conversion AS (
    SELECT
        "prj_resourcerequestskills_id",
        "prj_skill_id",
        "quantity",
        "prj_resourcerequest_id",
        {{add_meta_columns('resource_request_')}}
    FROM
        data_selection
)
SELECT
    s.*,
    s."resource_request_creation_time" AS "request_start_time",
    bpr."request_name",
    bpr."dm_note",
    bpr."pm_note",
    bpr."request_status",
    bpr."time_done" AS "request_done_time",
    bpr."time_end" AS "request_end_time",
    bpr."prj_project_id",
    sk."skill_name"
FROM
    data_type_rename_conversion AS s
LEFT JOIN base_prj_resourcerequests bpr
ON bpr.prj_resourcerequest_id = s.prj_resourcerequest_id
LEFT JOIN stg_skills sk
ON s.prj_skill_id = sk.prj_skill_id
