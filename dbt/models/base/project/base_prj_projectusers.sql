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
            'prj_projectusers'
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
        "Id" :: VARCHAR AS "prj_projectuser_id",
        "note" :: VARCHAR AS "project_user_note",
        "ispool" :: BOOLEAN AS is_pool,
        "status" :: INT AS "project_user_status",
        "userid" :: INT AS "prj_user_id",
        "isexpense" :: BOOLEAN AS "is_expense",
        "projectid" :: INT AS "project_id",
        "starttime" :: TIMESTAMP "user_start_time",
        "pmreportid" :: INT AS "pm_report_id",
        "projectrole" :: INT AS "project_role",
        "isfutureactive" :: BOOLEAN AS "is_future_active",
        "resourcerequestid" :: INT AS "resource_request_id",
        "allocatepercentage" :: FLOAT AS "allocate_percentage",
        {{ extract_meta_columns('employee_project_') }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
