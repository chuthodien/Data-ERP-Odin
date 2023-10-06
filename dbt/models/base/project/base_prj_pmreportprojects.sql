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
            'prj_pmreportprojects'
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
        "Id" :: VARCHAR AS "prj_pmreportprojects_id",
        "note" :: VARCHAR AS "report_note",
        "pmid" :: INT AS "prj_pm_id",
        "seen" :: BOOLEAN AS "is_seen",
        "status" :: INT AS "report_status",
        "ispunish" :: FLOAT AS "is_punish",
        "projectid" :: INT AS "prj_project_id",
        "pmreportid" :: INT AS "prj_pm_report_id",
        "totalnormalworkingtime" :: FLOAT AS "total_normal_working_time",
        "projecthealth" :: INT AS "project_health",
        "totalovertime" :: FLOAT AS "total_overtime",
        "timesendreport" :: TIMESTAMP AS "time_send_report",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
