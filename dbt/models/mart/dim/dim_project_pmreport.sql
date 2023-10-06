{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_project_pmreports AS (
    SELECT
        *
    FROM {{ ref('stg_project_pmreports')}}
),
dim_project AS (
    SELECT
        *
    FROM
        {{ ref('dim_project') }}
),
data_mapped AS (
    SELECT
        spp."prj_pmreport_id",
        spp."repport_name",
        spp."report_type",
        spp."year",
        spp."prj_pmreportprojects_id",
        spp."report_note",
        spp."prj_pm_id",
        spp."is_seen",
        spp."report_status",
        spp."is_punish",
        spp."prj_pm_report_id",
        spp."total_normal_working_time",
        spp."project_health",
        spp."total_overtime",
        spp."time_send_report",
        spp."date_at" AS "project_report_at",
        dp.*
    FROM stg_project_pmreports spp
    LEFT JOIN dim_project dp
    ON spp.prj_project_id = dp.prj_project_id
)
SELECT
    *
FROM
    data_mapped