{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH dim_employee_onboard AS (
    SELECT
        "date_at",
        "employee_email",
        'ONBOARD'::VARCHAR AS "event_type",
        '' AS "message_before",
        '' AS "message_after",
        '' AS "description",
        '' AS "reference_email",
        '' AS "reason" 
    FROM {{ref('dim_employee_onboard')}}
),
dim_employee_offboard AS (
    SELECT
        "date_at",
        "employee_email",
        'OFFBOARD'::VARCHAR AS "event_type",
        '' AS "message_before",
        '' AS "message_after",
        '' AS "description",
        '' AS "reference_email",
        '' AS "reason" 
    FROM {{ref('dim_employee_offboard')}}
),
dim_employee_promote AS (
    SELECT
        "date_at",
        "employee_email",
        'PROMOTE'::VARCHAR AS "event_type",
        "reviewer_note" AS "description",
        ('Level ' || "before_level_name") AS "message_before",
        ('Level ' || "after_level_name") AS "message_after",
        "reviewer_email" AS "reference_email",
        "review_type_name" AS "reason"
    FROM {{ref('dim_employee_level')}}
),
dim_employee_promote_detail AS (
    SELECT
        "date_at",
        "employee_email",
        ('PROMOTED_' || "after_level_code")::VARCHAR AS "event_type",
        "reviewer_note" AS "description",
        ('Level ' || "before_level_name") AS "message_before",
        ('Level ' || "after_level_name") AS "message_after",
        "reviewer_email" AS "reference_email",
        "review_type_name" AS "reason"
    FROM {{ref('dim_employee_level')}}
),
dim_employee_review AS (
    SELECT
        "date_at",
        "employee_email",
        'REVIEW'::VARCHAR AS "event_type",
        "reviewer_note" AS "description",
        ('Level ' || "before_level_name") AS "message_before",
        ('Level ' || "after_level_name") AS "message_after",
        "reviewer_email" AS "reference_email",
        "review_type_name" AS "reason"
    FROM {{ref('dim_employee_reviews')}} der
    WHERE der.review_status = 3
),
dim_employee_joined_project AS (
    SELECT
        "date_at",
        "employee_email",
        'JOINPROJECT'::VARCHAR AS "event_type",
        ('') AS "description",
        ('') AS "message_before",
        (project_name || ' - ' || project_code) AS "message_after",
        (pm_email) AS "reference_email",
        ('') AS "reason"
    FROM {{ref('dim_employee_joined_project')}}
),
employee_events AS (
    SELECT
        *
    FROM {{ref('employee_events')}}
),
data_mapped AS (
    SELECT
        "date_at",
        "employee_email",
        "event_type",
        "description",
        "message_before",
        "message_after",
        "reference_email",
        "reason"
    FROM dim_employee_onboard
    UNION SELECT * FROM dim_employee_offboard
    UNION SELECT * FROM dim_employee_promote
    UNION SELECT * FROM dim_employee_review
    UNION SELECT * FROM dim_employee_joined_project
    UNION SELECT * FROM dim_employee_promote_detail
)
SELECT
    dm.*,
    et.*
FROM data_mapped dm
LEFT JOIN employee_events et
    ON dm.event_type = et.employee_event_code