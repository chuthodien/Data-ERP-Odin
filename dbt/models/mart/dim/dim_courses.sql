{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH stg_courses AS (
    SELECT
        *
    FROM
        {{ref('stg_lms_courses')}}
),

dim_course_instances AS (
    SELECT
        *
    FROM
        {{ ref('stg_lms_course_instances') }}
)

SELECT
    sci.*,
    courseinstances_id as course_instances_id,
    status,
    start_time,
    end_time,
    allowskip as allow_skip,
    totalquiz,
    passingmark as passing_mark,
    numberdaytostudy as number_day_to_study,
    allowfinalquizretry as allow_final_quiz_retry,
    enablecoursegradingscheme as enable_course_grading_scheme
FROM
    dim_course_instances dci    
LEFT JOIN stg_courses sci
    ON dci.course_id = sci.course_id