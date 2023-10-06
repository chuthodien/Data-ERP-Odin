{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('stg_resource_bills') }}
),
dim_project AS (

    SELECT
        *
    FROM
        {{ ref('dim_project') }}
)
SELECT
    dp.*,
    s."resource_bill_creation_time" as "bill_creation_time",
    s."prj_timesheetprojectbills_id",
    s."bill_note",
    s."prj_user_id",
    s."account_end_time",
    s."bill_rate",
    s."bill_role",
    s."is_active",
    s."project_id",
    s."bill_start_time",
    s."shadow_note",
    s."prj_timesheet_id",
    s."working_time"
FROM
    source s
LEFT JOIN dim_project dp
ON dp.prj_project_id = s.project_id

