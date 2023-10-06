{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('stg_account_bills') }}
    WHERE account_bill_is_deleted is false
),
dim_project AS (

    SELECT
        *
    FROM
        {{ ref('dim_project') }}
),
employee AS (
    SELECT
        *
    FROM
        {{ ref('stg_project_users') }}
)
SELECT
    dp.*,
    s."account_bill_creation_time" as "bill_creation_time",
    s."prj_projectuserbills_id",
    s."user_bill_note",
    s."prj_user_id",
    CASE
        WHEN (s.account_end_time = '0001-01-01 00:00:00') 
        THEN NULL
        ELSE account_end_time
    END
        AS account_end_time,
    s."bill_rate",
    s."bill_role",
    s."is_active",
    s."account_start_time",
    s."shadow_note"
FROM
    source s
LEFT JOIN dim_project dp
ON dp.prj_project_id = s.prj_project_id
JOIN employee em
ON s.prj_user_id = em.prj_user_id

