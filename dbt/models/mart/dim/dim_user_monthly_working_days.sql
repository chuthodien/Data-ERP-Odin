{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH source AS (
    SELECT
        *
    FROM
        {{ref('stg_member_project_tsh')}}
),
daily_time AS (
    SELECT
        time_at,
        tsh_user_id,
        SUM(CAST(ROUND(working_time / 1000 / 60 / 60 / 0.25, 2) AS INT)) * 0.25 AS working_hours,
        {{dbt_utils.date_trunc('month', "time_at")}} AS time_at_month
    FROM 
        source
    GROUP BY time_at_month, time_at, tsh_user_id
),
monthly_time AS (
    SELECT
        tsh_user_id,
        CAST(ROUND(SUM(working_hours) / 8 / 0.25, 2) AS INT) * 0.25 AS working_days,
        time_at_month
    FROM
        daily_time
    GROUP BY tsh_user_id, time_at_month
    ORDER BY time_at_month, tsh_user_id
)
SELECT
    *
FROM
    monthly_time


