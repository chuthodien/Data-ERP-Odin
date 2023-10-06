{{ config(
    schema = 'mart',
    materialized = "view"
) }}

WITH project_bill_dimension AS (
    SELECT
        *,
        count(prj_timesheetprojectbills_id) over (PARTITION BY prj_user_id, project_id order by start_month_date ASC) account_m_order,
        count(prj_timesheetprojectbills_id) over (PARTITION BY prj_user_id, project_id order by start_month_date DESC) account_m_rev_order 
    FROM
        {{ ref('dim_project_bill') }}
)

SELECT
    ptd.*,
    (CASE
            WHEN ptd."account_m_order" = 1 THEN true
            ELSE false
        END)::BOOLEAN AS "is_new",
    (CASE
            WHEN ptd."account_m_rev_order" = 1 THEN true
            ELSE false
        END)::BOOLEAN AS "is_ended"
FROM
    project_bill_dimension ptd
WHERE
    ptd.is_active = 1 
    AND ptd.working_time > 5
    AND ptd.is_deleted = false
    -- AND (ptd.preverse_start_time IS NOT NULL OR ptd.preverse_end_time IS NOT NULL)
