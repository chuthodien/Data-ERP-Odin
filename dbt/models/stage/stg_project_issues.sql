{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH base_prj_pmreportprojectissues AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_prj_pmreportprojectissues'
        ) }}
),
stg_project_pmreports AS (
    SELECT
        *
    FROM
        {{ ref(
            'stg_project_pmreports'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "prj_pmreportprojectissues_id",
        "impact",
        "source",
        "issue_status",
        "critical",
        "solution",
        "description",
        "meeting_solution",
        "pm_report_project_id",
        spp."prj_project_id",
        {{add_meta_columns('project_issue_')}}
    FROM
        base_prj_pmreportprojectissues bpp
    LEFT JOIN stg_project_pmreports spp
    ON bpp.pm_report_project_id = spp.prj_pmreportprojects_id
)
SELECT
    *
FROM
    data_type_rename_conversion
