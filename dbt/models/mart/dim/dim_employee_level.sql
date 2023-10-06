{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH dim_employee_reviews AS (
    SELECT
        *
    FROM {{ref('dim_employee_reviews')}}
    WHERE review_status = 3
        AND level_gap > 0
)
SELECT
    {{add_date_at('applied_date')}},
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
    before_level_code,
    before_level_name,
    after_level_code,
    after_level_name,
    review_type_name,
    review_type_code,
    review_status_name,
    review_status_code
FROM
    dim_employee_reviews