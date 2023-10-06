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
            'prj_timesheetprojects'
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
        "Id" :: VARCHAR AS "prj_timesheetprojects_id",
        "note" :: VARCHAR AS "timesheet_project_note",
        "projectid" :: INT AS "project_id",
        "iscomplete" :: INT AS "is_complete",
        "timesheetid" :: INT AS "timesheet_id",
        "projectbillinfomation" :: VARCHAR AS "project_bill_infomation",
        {{ extract_meta_columns('project_account_') }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
