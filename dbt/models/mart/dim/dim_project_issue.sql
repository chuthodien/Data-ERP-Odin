{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_project_issues AS (
    SELECT
        *
    FROM {{ ref('stg_project_issues')}}
),
dim_project AS (
    SELECT
        *
    FROM
        {{ ref('dim_project') }}
),
data_mapped AS (
    SELECT
        spp."prj_pmreportprojectissues_id",
        spp."impact",
        spp."source",
        spp."issue_status",
        spp."critical",
        spp."solution",
        spp."description",
        spp."meeting_solution",
        spp."pm_report_project_id",
        spp."project_issue_is_deleted",
        spp."project_issue_creation_time",
        spp."project_issue_deletion_time",
        spp."project_issue_last_modification_time",
        dp.*
    FROM stg_project_issues spp
    LEFT JOIN dim_project dp
    ON spp.prj_project_id = dp.prj_project_id
)
SELECT
    *
FROM
    data_mapped