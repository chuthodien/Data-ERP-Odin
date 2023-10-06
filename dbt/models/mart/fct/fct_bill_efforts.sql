{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH working_time AS(

    SELECT
        SUM(working_hour) AS total_working_hour,
        CAST(SUM(working_hour) / 8 AS FLOAT) AS working_time,
        COUNT(
            DISTINCT user_id
        ) AS total_user,
        month_start_date,
        project_code
    FROM
        {{ ref('fct_bill_workingtime') }}
    WHERE
        "status" = 2
    GROUP BY
        project_code,
        month_start_date
),
account_cnt AS (
    SELECT
        COUNT(*) AS total_account,
        SUM(working_time) AS billed_working_time,
        start_month_date AS month_start_date,
        total_working_day,
        project_id
    FROM
        {{ ref('fct_project_bill') }}
    WHERE
        is_deleted IS FALSE
    GROUP BY
        project_id,
        start_month_date,
        total_working_day
),
dim_project AS (
    SELECT
        *
    FROM
        {{ ref('dim_project') }}
),
mapped_account_cnt AS(
    SELECT
        account_cnt.*,
        dim_project.project_code,
        dim_project.client_code
    FROM
        account_cnt
        LEFT JOIN dim_project
        ON account_cnt.project_id = dim_project.prj_project_id
),
data_mapped AS(
    SELECT
        wt.total_working_hour,
        ROUND(
            (
                wt.working_time / ac.total_working_day
            ) :: numeric,
            2
        ) AS man_month,
        ROUND(
            (
                ac.billed_working_time / ac.total_working_day
            ) :: numeric,
            2
        ) AS billed_man_month,{# m.working_days_of_month,
        m.date_month,#} wt.total_user,
        ac.total_account,
        COALESCE(
            ac.month_start_date,
            wt.month_start_date
        ) AS month_start_date,
        COALESCE(
            ac.project_code,
            wt.project_code
        ) AS project_code
    FROM
        mapped_account_cnt AS ac 
        FULL OUTER JOIN working_time AS wt
        ON wt.month_start_date = ac.month_start_date
        AND wt.project_code = ac.project_code
)
SELECT
    *
FROM
    data_mapped
