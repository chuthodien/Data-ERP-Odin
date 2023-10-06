{{ config(
    schema = 'mart',
    materialized = "table"
) }}


WITH fct_employee_branch_daily AS (
    SELECT
        *
    FROM
        {{ ref('fct_employee_branch_daily') }}
),
dim_month AS (
    SELECT
        *
    FROM
        {{ ref('dim_month') }}
),
dim_branch AS (
    SELECT
        *
    FROM
        {{ ref('dim_branch') }}
),
data_month_branch AS (
    SELECT
        *
    FROM
        dim_month dm
    LEFT JOIN 
        dim_branch db
    ON dm.date_month is not null
),
data_aggrated AS (
    SELECT 
        febd.month_start_date AS date_at,
        febd.branch_id AS branch,
        SUM(febd.total_onboard) AS total_onboard,
        SUM(febd.total_offboard) AS total_offboard,
        SUM(febd.total_staff_offboard) AS total_staff_offboard,
        SUM(febd.total_intern_offboard) AS total_intern_offboard,
        SUM(febd.employee_change) AS employee_change
    FROM fct_employee_branch_daily febd
        GROUP BY febd.month_start_date, febd.branch_id
),
data_aggrated_2 AS (
    SELECT 
        dmb.*,
        da.*
    FROM data_aggrated da
    LEFT JOIN data_month_branch dmb
        ON da.branch = dmb.branch_id AND da.date_at = dmb.date_month
),
data_aggrated_3 AS (
    SELECT 
        *,
        SUM(employee_change) OVER (PARTITION BY branch_id order by date_at) total_employee 
    FROM data_aggrated_2
)
SELECT 
    *,
    (COALESCE(total_offboard / NULLIF(total_employee,0), 0))::FLOAT AS all_drop_rate,
    (COALESCE(total_staff_offboard / NULLIF(total_employee,0), 0))::FLOAT AS staff_drop_rate,
    (COALESCE(total_intern_offboard / NULLIF(total_employee,0), 0))::FLOAT AS intarn_drop_rate
FROM data_aggrated_3
