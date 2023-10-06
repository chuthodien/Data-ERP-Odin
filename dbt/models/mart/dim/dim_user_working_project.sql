{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH user_contribution AS (
    SELECT
        {{dbt_utils.date_trunc('month','time_at')}} AS time_at_month,
        SUM({{ms_to_hour('working_time')}}) AS working_hour,
        tsh_project_id,
        LOWER(tsh_project_code) as tsh_project_code,
        tsh_user_id
    FROM
        {{ ref(
            'stg_member_project_tsh'
        ) }}
    GROUP BY tsh_project_id, tsh_project_code, tsh_user_id, {{dbt_utils.date_trunc('month','time_at')}}
    ORDER BY tsh_project_id, time_at_month
),
dim_month AS (
    SELECT
        *
    FROM 
        {{ref(
            'dim_month'
        )}}
),
project_timesheet_bill AS (
    SELECT 
        COUNT(source.prj_timesheetprojectbills_id) AS count_project_timesheet_bill,
        source.year,
        source.month,
        ROUND(SUM({{ project_income("source.bill_rate", "source.working_time", "source.charge_type", "dm.working_days_of_month") }})) :: FLOAT AS project_income,
        source.project_id,
        source.currency_id,
        source.start_month_date
    FROM
        {{ref(
            'stg_project_bill'
        )}} source
    LEFT JOIN dim_month dm ON source.start_month_date = dm.date_month
    WHERE is_deleted = false AND is_active :: BOOLEAN = true AND working_time > 0
    GROUP BY project_id, year, month, start_month_date, currency_id
    ORDER BY project_id, start_month_date
),
project AS (
    SELECT 
        prj_project_id,
        LOWER(project_code) as project_code,
        project_name,
        project_status,
        start_time,
        end_time,
        project_type
    FROM
        {{ref(
            'stg_projects'
        )}}
),
data_mapped AS (
    SELECT
        uc.tsh_project_id,
        uc.tsh_user_id,
        uc.working_hour,
        uc.time_at_month,
        uc.tsh_project_code,
        project.prj_project_id,
        uc.tsh_project_code as project_code,
        COALESCE(project.project_name, 'Noname') as project_name,
        COALESCE(project.project_status, 1) as project_status,
        COALESCE(project.start_time, null) as start_time,
        COALESCE(project.end_time, null) as end_time,
        COALESCE(project.project_type, 3) as project_type,
        ptb.start_month_date,
        COALESCE(ptb.count_project_timesheet_bill,0) AS count_project_timesheet_bill,
        COALESCE(ptb.year, EXTRACT(YEAR FROM uc.time_at_month)) AS year,
        COALESCE(ptb.month, EXTRACT(MONTH FROM uc.time_at_month)) AS month,
        COALESCE(ptb.project_income, 0) :: FLOAT AS project_income,
        COALESCE(ptb.currency_id, 2) AS currency_id
    FROM user_contribution uc
    LEFT JOIN project
    ON uc.tsh_project_code =  project.project_code
    LEFT JOIN project_timesheet_bill ptb
    ON project.prj_project_id = ptb.project_id AND uc.time_at_month = ptb.start_month_date
    ORDER BY prj_project_id, year, month
)
SELECT
    *
FROM
    data_mapped
