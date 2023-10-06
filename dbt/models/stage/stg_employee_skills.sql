{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (
    SELECT * FROM {{ ref('base_prj_userskills' ) }}
),
users AS (
    SELECT 
        "prj_user_id",
        "email"
    FROM {{ ref('stg_employee' ) }}
),
data_mapped AS (
    SELECT
        {{add_date_at('deletion_time')}},
        s."prj_user_id",
        "email" AS "employee_email",
        "prj_userskill_id",
        "prj_skill_id",
        {{add_meta_columns('employee_skill_')}}
    FROM
        source s
    INNER JOIN users
        ON s.prj_user_id = users.prj_user_id
)
SELECT
    *
FROM
    data_mapped
