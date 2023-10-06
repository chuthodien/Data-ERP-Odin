{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (
    SELECT 
        *
    FROM 
        {{ref('stg_review_intern_details')}}
),
employee AS (
    SELECT
        *
    FROM 
        {{ref('stg_employee')}}
),
review_status AS (
    SELECT 
        *
    FROM
        {{ref('review_status')}}
),
review_types AS (
    SELECT 
        *
    FROM 
        {{ref('review_types')}}
),
employee_levels AS (
    SELECT 
        *
    FROM
        {{ref('employee_levels')}}
),
data_mapped AS (
    SELECT 
        source.*,
        reviewer.fullname AS reviewer_fullname,
        reviewer.email AS reviewer_email,
        COALESCE(internship.email, concat('Intern ', source.internship_id, ' not found')) AS internship_email,
        COALESCE(internship.fullname, 'Unknown') AS internship_fullname, 
        review_status.review_status_code,
        review_types.review_type_code,
        before_level.employee_level_code AS before_level_code,
        after_level.employee_level_code AS after_level_code
    FROM source
    JOIN employee reviewer ON source.reviewer_id = reviewer.tsh_user_id
    LEFT JOIN employee internship ON source.internship_id = internship.tsh_user_id
    LEFT JOIN review_status ON source.review_intern_status = review_status.review_status_id
    LEFT JOIN review_types ON source.review_intern_type = review_types.review_type_id 
    LEFT JOIN employee_levels before_level ON source.before_level = before_level.employee_level_id
    LEFT JOIN employee_levels after_level ON source.after_level = after_level.employee_level_id
)
SELECT
    *
FROM 
    data_mapped
