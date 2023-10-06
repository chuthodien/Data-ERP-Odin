{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH base_prj_timesheetprojects AS (
    SELECT
        *
    FROM
     {{ ref('base_prj_timesheetprojects' )}}
),
data_mapped AS (
    SELECT
        *
    FROM base_prj_timesheetprojects
)
SELECT
    *
FROM
    data_mapped
