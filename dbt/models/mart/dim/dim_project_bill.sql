{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH project_bill_dimension AS (
    SELECT
        *
    FROM
        {{ ref('stg_project_bill') }}
),
account_bill AS (
    SELECT
        DISTINCT
        pm_email,
        prj_project_id,
		pm_user_id
    FROM {{ref('dim_account_bill')}}
),
preverse_time_dimension AS (
    SELECT
        pbd.*,
        CASE
            WHEN (pbd.bill_start_time = '0001-01-01 00:00:00') 
            THEN NULL
            ELSE bill_start_time
        END
            AS preverse_start_time,
        CASE
            WHEN (pbd.account_end_time = '0001-01-01 00:00:00') 
            THEN NULL
            ELSE account_end_time
        END
            AS preverse_end_time,
        ab.pm_user_id,
        ab.pm_email
    FROM
        project_bill_dimension pbd
    JOIN account_bill ab 
    ON pbd.project_id = ab.prj_project_id
)

SELECT
    ptd.*
FROM
    preverse_time_dimension ptd
WHERE
    ptd.is_active = 1 
    AND ptd.working_time > 5
    -- AND ptd.is_deleted = false
    -- AND (ptd.preverse_start_time IS NOT NULL OR ptd.preverse_end_time IS NOT NULL)