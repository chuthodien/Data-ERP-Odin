{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_employee_reviews AS (
    SELECT
        "date_at",
        {{add_date_at('last_modification_time', 'applied_date')}},
        "before_level",
        "reviewer_email",
        "after_level",
        "review_intern_status",
        internship_email AS reviewee_email,
        internship_email AS employee_email,
        0 AS "review_type",
        review_intern_note AS "reviewer_note",
        '' AS "reviewee_note",
        review_intern_status AS "review_status",
        (after_level - before_level) AS level_gap
    FROM {{ref('stg_employee_reviews')}}
    WHERE review_intern_status IN (2,3)
),
employee_levels AS (
    SELECT
        *
    FROM
        {{ref('employee_levels')}}
),
review_types AS (
    SELECT
        *
    FROM
        {{ref('review_types')}}
),
review_status AS (
    SELECT
        *
    FROM
        {{ref('review_status')}}
)
SELECT
    date_at,
    applied_date,
    employee_email,
    reviewee_email,
    reviewer_email,
    before_level,
    after_level,
    review_type,
    review_status,
    reviewer_note,
    reviewee_note,
    level_gap,
    el.employee_level_code AS before_level_code,
    el.employee_level_name AS before_level_name,
    el2.employee_level_code AS after_level_code,
    el2.employee_level_name AS after_level_name,
    rt.*,
    rst.*
FROM stg_employee_reviews ser
LEFT JOIN review_types rt
    ON rt.review_type_id = ser.review_type
LEFT JOIN review_status rst
    ON rst.review_status_id = ser.review_status
LEFT JOIN employee_levels el
    ON ser.before_level = el.employee_level_id
LEFT JOIN employee_levels el2
    ON ser.after_level = el2.employee_level_id
