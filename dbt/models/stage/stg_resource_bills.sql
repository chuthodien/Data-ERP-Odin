
{{ config(
    materialized = 'table',
    schema = 'base'
) }}

WITH base_prj_timesheetprojectbills AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_prj_timesheetprojectbills'
        ) }}
),
base_prj_timesheets AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_prj_timesheets'
        ) }}
), 
base_prj_timesheetprojects AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_prj_timesheetprojects'
        ) }}
),
data_selection AS (
    SELECT
        *
    FROM
        base_prj_timesheetprojectbills
),
data_type_rename_conversion AS (
    SELECT
        "prj_timesheetprojectbills_id",
        "bill_note",
        "prj_user_id",
        "account_end_time",
        "bill_rate",
        "bill_role",
        "is_active",
        "project_id",
        "bill_start_time",
        "shadow_note",
        "prj_timesheet_id",
        "working_time",
        {{add_meta_columns('resource_bill_')}}
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
