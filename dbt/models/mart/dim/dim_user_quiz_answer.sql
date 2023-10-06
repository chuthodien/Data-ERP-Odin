{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (
    SELECT 
        * 
    FROM 
        {{ref('stg_user_quizs')}}
),
komu_user AS (
    SELECT 
        * 
    FROM 
        {{ref('stg_employee')}}
),
user_level AS (
    SELECT
        *
    FROM 
        {{ref('stg_employee_levels')}}
),
branch AS (
    SELECT
        *
    FROM    
        {{ref('stg_branch')}}
),
data_mapped AS (
    SELECT 
        source.*,
        u.hrm_user_id,
        COALESCE(u.email, 'Unknown') AS email,
        COALESCE(u.fullname, 'Unknown') AS fullname,
        l.level_name AS employee_level,
        u.employee_type,
        b.branch_name AS employee_branch,
        COALESCE({{dbt_utils.date_trunc('month', "source.update_at")}}, {{dbt_utils.date_trunc('month', "source.creation_time")}}) AS start_month_date
    FROM source
    LEFT JOIN komu_user u ON source.komu_user_id = u.komu_user_id
    LEFT JOIN user_level l ON u.employee_level = l.level_id 
    LEFT JOIN branch b ON u.branch = b.branch_id
)
SELECT 
    *
FROM 
    data_mapped