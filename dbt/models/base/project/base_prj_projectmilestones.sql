{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_project',
            'prj_projectmilestones'
        ) }}
),
data_selection AS (
    SELECT
        *
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: VARCHAR AS "prj_projectmilestones_id",
        "status" :: INT AS "milestone_status",
        "projectid" :: INT AS "prj_project_id",
        "uattimeend" :: TIMESTAMP AS "uat_time_end",
        "description" :: text AS "milestone_description",
        "uattimestart" :: TIMESTAMP AS "uat_time_start",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
