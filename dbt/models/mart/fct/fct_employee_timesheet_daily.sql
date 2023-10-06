{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_timesheets AS (
    SELECT
        *
    FROM
        {{ ref('dim_timesheet') }}
),
dim_approved AS (
   SELECT
    date_at,
    employee_email,
    SUM(working_time) AS approved_time
    FROM
        dim_timesheets
    WHERE status = 2
    GROUP BY employee_email, date_at

),
dim_rejected AS (
    SELECT
        date_at,
        employee_email,
        SUM(working_time) AS rejected_time
    FROM
        dim_timesheets
    WHERE status = 0
    GROUP BY employee_email, date_at
),
dim_sumitted AS (
    SELECT
        date_at,
        employee_email,
        SUM(working_time) AS  submited_time
    FROM
        dim_timesheets
    WHERE status =1
    GROUP BY employee_email, date_at
),
data_mapped AS (
    SELECT
        COALESCE(da.date_at, dr.date_at, ds.date_at) as date_at,
        COALESCE(da.employee_email, dr.employee_email, ds.employee_email) AS employee_email,
        COALESCE(da.approved_time, 0) as approved_time,
        COALESCE(dr.rejected_time, 0) as rejected_time,
        COALESCE(ds.submited_time, 0) as submited_time
    FROM dim_approved da
    FULL OUTER JOIN dim_rejected dr 
        ON da.employee_email = dr.employee_email
        AND da.date_at = dr.date_at
    FULL OUTER JOIN dim_sumitted ds
        ON da.employee_email = ds.employee_email
        AND da.date_at = ds.date_at
),
data_aggrated AS (
    SELECT 
        *
    FROM
        data_mapped 
)
SELECT * from data_aggrated