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
)
SELECT
    project_code,
    prj_project_id,
    SUM({{ user_percent_contribution("user_working_time_project", "to_salary_month", "project_type", "project_name", "user_level", "retro_point") }}) AS cost_project_month,
    SUM({{ user_percent_sqrt_contribution("user_working_time_project", "to_salary_month", "project_type", "project_name", "user_level", "retro_point") }}) AS cost_sqrt_project_month,
    time_at_month,
    year,
    month
FROM source
GROUP BY project_code, prj_project_id, year, month, time_at_month
