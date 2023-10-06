{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_employee AS (
    SELECT
        *
    FROM
        {{ ref('dim_employee') }}
),
dim_date AS (
    SELECT
        *
    FROM 
    {{ ref('dim_date') }}
),
stg_timesheets AS (
    SELECT
        *
    FROM
        {{ ref('stg_timesheets') }}
),
dim_task AS (
    SELECT
        *
    FROM
        {{ ref('dim_task') }}
),
dim_project_keep AS (
    SELECT
        *
    FROM
        {{ ref('dim_project_keep') }}
),
work_types AS (
    SELECT
        *
    FROM
        {{ ref('work_types') }}
),
data_mapped AS (
    SELECT
        dts.*,
        dt."task_name",
        dt."task_type",
        dpk."tsh_project_code",
        dpk."project_name",
        dpk."project_note",
        dpk."tsh_project_status",
        dpk."end_end",
        dpk."start_time",
        dpk."tsh_customer_id",
        dpk."tsh_project_type",
        dpk."is_general",
        wt.*
    FROM stg_timesheets dts
    LEFT JOIN dim_task dt
        ON dts.tsh_task_id = dt.tsh_task_id
    LEFT JOIN dim_project_keep dpk
       ON dts.tsh_project_id = dpk.tsh_project_id
    LEFT JOIN work_types wt
        ON wt.work_type_id = dts.type_of_work
),
data_aggrated AS (
    SELECT
        *
    FROM
        data_mapped 
)
SELECT * from data_aggrated
