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
            'prj_pmreportprojectissues'
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
        "Id" :: VARCHAR AS "prj_pmreportprojectissues_id",
        "impact" :: VARCHAR AS "impact",
        "Source" :: VARCHAR AS "source",
        "status" :: INT AS "issue_status",
        "critical" :: VARCHAR AS "critical",
        "solution" :: VARCHAR "solution",
        "description" :: text "description",
        "meetingsolution" :: VARCHAR AS "meeting_solution",
        "pmreportprojectid" :: VARCHAR AS "pm_report_project_id",
        {{ extract_meta_columns() }}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
