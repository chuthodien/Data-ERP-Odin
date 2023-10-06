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
            'prj_resourcerequests'
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
        "Id" :: INT AS "prj_resourcerequest_id",
        "Name" :: VARCHAR AS "request_name",
        "dmnote" :: VARCHAR AS "dm_note",
        "pmnote" :: VARCHAR AS "pm_note",
        "status" :: INT AS "request_status",
        "timedone" :: TIMESTAMP AS "time_done",
        "timeneed" :: TIMESTAMP AS "time_end",
        "projectid" :: INT AS "prj_project_id",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
