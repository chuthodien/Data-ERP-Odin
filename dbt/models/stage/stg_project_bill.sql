{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH base_prj_timesheetprojectbills AS (
    SELECT
        *
    FROM
     {{ ref('base_prj_timesheetprojectbills' )}}
),
base_prj_timesheets AS (
    SELECT
        *
    FROM
     {{ ref('base_prj_timesheets' )}}
    WHERE is_deleted = false
),
data_mapped AS (
    SELECT
        base_prj_timesheetprojectbills.*,
        base_prj_timesheets.year,
        base_prj_timesheets.month,
        base_prj_timesheets.start_month_date,
        base_prj_timesheets.total_working_day
    FROM base_prj_timesheetprojectbills
    JOIN base_prj_timesheets
        ON base_prj_timesheetprojectbills.prj_timesheet_id::varchar = base_prj_timesheets.prj_timesheets_id
)
SELECT
    *
FROM
    data_mapped
