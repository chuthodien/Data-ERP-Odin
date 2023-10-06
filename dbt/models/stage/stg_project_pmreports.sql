{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *,
        {{add_meta_columns('project_pmreport_')}}
    FROM
        {{ ref(
            'base_prj_pmreports'
        ) }}
),
base_prj_pmreportprojects AS (
    SELECT 
        *,
        {{add_meta_columns('prj_pmreportproject_')}}
    FROM
        {{ ref(
            'base_prj_pmreportprojects'
        ) }} 
),
data_type_rename_conversion AS (
    SELECT
        "prj_pmreport_id",
        "repport_name",
        "report_type",
        "year",
        "prj_pmreportprojects_id",
        bpp."report_note",
        "prj_pm_id",
        "is_seen",
        bpp."report_status",
        "is_punish",
        "prj_project_id",
        "prj_pm_report_id",
        "total_normal_working_time",
        "project_health",
        "total_overtime",
        "time_send_report",
        {{add_date_at('time_send_report')}}
    FROM
        source s
    LEFT JOIN base_prj_pmreportprojects bpp
    ON s.prj_pmreport_id = bpp.prj_pm_report_id
)
SELECT
    *
FROM
    data_type_rename_conversion
