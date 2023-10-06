{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (
    SELECT
        "date_at",
        "employee_email",
        "tsh_user_id",
        (
            CASE 
            WHEN "checkin_time" = '00:00'
            THEN "register_checkin"
            ELSE "checkin_time"
            END
        ) AS "checkin_time",
        (
            CASE 
            WHEN "checkout_time" = '00:00'
            THEN "register_checkout"
            ELSE "checkout_time"
            END
        ) AS "checkout_time",
        "is_locked",
        "user_note",
        "is_deleted",
        "note_reply",
        "creation_time",
        "deletion_time",
        "register_checkin",
        "register_checkout",
        "is_punished_checkin",
        "is_punished_checkout"
    FROM
        {{ ref(
            'stg_timekeepings'
        ) }}
)
SELECT
    *
FROM
    source
