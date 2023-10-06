{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (
    SELECT
        *
    FROM
        {{ref(
            'dim_user_contribution'
        )}}
),
cost_project AS (
    SELECT  
        *
    FROM
        {{ref(
            'fct_cost_project'
        )}}
),
level_code AS (
    SELECT
        *
    FROM
        {{ref('stg_employee_levels')}}
),
project_contribution_static_data AS (
    SELECT
        *
    FROM
        {{ref(
            'project_bonus'
        )}}
),
nonbill_project_bonus AS (
    SELECT
        *
    FROM
        {{ref(
            'nonbill_project_bonus'
        )}}
),
internal_project_cost_of_month AS (
    SELECT 
        SUM({{user_percent_contribution("user_working_time_project", "to_salary_month", "project_type", "project_name", "user_level", "retro_point")}}) AS cost_of_month,
        COUNT( DISTINCT prj_project_id) AS project_quantity,
        time_at_month
    FROM source
    WHERE have_project_timesheet_bill = false
    GROUP BY time_at_month
),
income_month AS (
    SELECT 
        SUM(project_income) / 100 * npb.percent AS internal_projects_bonus,
        time_at_month
    FROM 
    (
        SELECT DISTINCT
            prj_project_id,
            project_income,
            time_at_month,
            have_project_timesheet_bill
        FROM source
    ) AS re
    LEFT JOIN nonbill_project_bonus npb ON time_at_month IS NOT NULL
    WHERE have_project_timesheet_bill = true
    GROUP BY time_at_month, percent
),
data_mapped AS (
    SELECT
        source.*,
        le.level_code,
        (
            CASE
                WHEN source.have_project_timesheet_bill = true 
                THEN source.project_income * (pcsd.percent_project :: FLOAT/100)
                ELSE im.internal_projects_bonus * ( cost_project.cost_project_month/ipcom.cost_of_month ) 
            END
        ) :: FLOAT AS project_bonus,
        (
            CASE
                WHEN cost_project.cost_project_month > 0 THEN ({{user_percent_contribution("source.user_working_time_project", "source.to_salary_month", "source.project_type", "source.project_name", "source.user_level", "source.retro_point")}}/cost_project.cost_project_month * 100)
                ELSE 0
            END
        ) :: FLOAT AS percent_user_contribution,
        (
            CASE
                WHEN cost_project.cost_project_month > 0 THEN ({{user_percent_sqrt_contribution("source.user_working_time_project", "source.to_salary_month", "source.project_type", "source.project_name", "source.user_level", "source.retro_point")}}/cost_project.cost_sqrt_project_month * 100)
                ELSE 0
            END
        ) :: FLOAT AS percent_user_sqrt_contribution
    FROM
        source
    LEFT JOIN cost_project
    ON source.project_code = cost_project.project_code AND source.year = cost_project.year AND source.month = cost_project.month
    LEFT JOIN project_contribution_static_data pcsd
    ON source.project_type = pcsd.project_type_id
    LEFT JOIN income_month im
    ON im.time_at_month = source.time_at_month AND source.have_project_timesheet_bill = false
    LEFT JOIN internal_project_cost_of_month ipcom
    ON ipcom.time_at_month = source.time_at_month AND source.have_project_timesheet_bill = false AND ipcom.cost_of_month > 0
    JOIN level_code le
    ON source.user_level=le.level_id
    ORDER BY source.prj_project_id, source.year, source.month, source.tsh_user_id
),
data_selection AS (
    SELECT
        *,
        (project_bonus * percent_user_contribution/100) :: FLOAT AS user_project_bonus,
        (project_bonus * percent_user_sqrt_contribution/100) :: FLOAT AS user_sqrt_project_bonus
    FROM 
        data_mapped
)
SELECT 
    *
FROM
    data_selection
