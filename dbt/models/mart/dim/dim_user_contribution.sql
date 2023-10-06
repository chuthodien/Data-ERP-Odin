{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (
    SELECT
        *
    FROM
        {{ref(
            'dim_user_working_project'
        )}}
),
dim_month AS (
    SELECT
        *
    FROM 
        {{ref(
            'dim_month'
        )}}
),
member AS (
    SELECT
        * 
    FROM
        {{ref(
            'stg_employee'
        )}}
),
user_salary_month AS (
    SELECT
        *
    FROM
        {{ref(
            'stg_user_monthly_salary'
        )}}
),
retro_points AS (
    SELECT
        *
    FROM 
        {{ref(
            'stg_retro_points'
        )}}
),
default_point AS (
    SELECT
        tsh_project_code,
        start_date,
        AVG(point) AS avg_point
    FROM retro_points
    GROUP BY tsh_project_code, start_date
),
data_mapped AS (
    SELECT
        source.prj_project_id,
        source.project_code,
        source.project_name,
        source.tsh_user_id,
        {{ hour_to_month("source.working_hour","dm.working_days_of_month") }} :: FLOAT AS user_working_time_project,
        source.time_at_month,
        source.year,
        source.month,
        source.currency_id,
        (
            CASE
                WHEN source.currency_id = 2 THEN {{ usd_to_vnd("source.project_income") }}
                WHEN source.currency_id = 3 THEN project_income
                WHEN source.currency_id = 4 THEN {{ aud_to_vnd("source.project_income") }}
            END
        ) AS project_income,
        (
            CASE
                WHEN source.count_project_timesheet_bill=0 THEN false
                ELSE true
            END
        ) :: BOOLEAN AS have_project_timesheet_bill,
        COALESCE(member.fullname, 'Unknown') AS user_fullname,
        COALESCE(member.email,'Unknown') AS email,
        member.is_deleted AS user_deleted,
        member.status AS user_status,
        source.project_status,
        source.start_time,
        source.end_time,
        source.project_type,
        member.hrm_user_id,
        usm.user_level,
        COALESCE(usm.employee_level_code, 'Unknown') AS employee_level_code,
        COALESCE(usm.to_salary, 0) :: INT AS to_salary_month,
        COALESCE(rp.point, dp.avg_point, 3) :: FLOAT AS retro_point
    FROM source
    INNER JOIN member
        ON source.tsh_user_id = member.tsh_user_id 
    LEFT JOIN user_salary_month usm
        ON member.hrm_user_id = usm.employee_id AND source.time_at_month = usm.time_at_month
    LEFT JOIN dim_month dm
        ON source.time_at_month = dm.date_month
    LEFT JOIN retro_points rp
        ON rp.tsh_project_code = source.project_code AND rp.tsh_user_id = source.tsh_user_id AND source.time_at_month = rp.start_date
    LEFT JOIN default_point dp
        ON dp.tsh_project_code = source.project_code AND source.time_at_month = dp.start_date
    WHERE source.working_hour > 0
)
SELECT 
    *
FROM 
    data_mapped
ORDER BY year, month, prj_project_id, tsh_user_id
