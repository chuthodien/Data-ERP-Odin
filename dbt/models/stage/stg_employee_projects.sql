{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH base_prj_projectusers AS (
    SELECT
        *
        ,{{add_date_at('user_start_time', 'user_start_date')}}
    FROM
     {{ ref('base_prj_projectusers' )}}
), stg_employee AS (
    SELECT
        *
    FROM
     {{ ref('stg_employee' )}}
),
data_mapped AS (
    SELECT
        bpp.*
        ,se.email AS employee_email
    FROM
        base_prj_projectusers bpp
    LEFT JOIN stg_employee se 
        ON bpp.prj_user_id = se.prj_user_id
)
SELECT
    *
FROM
    data_mapped
