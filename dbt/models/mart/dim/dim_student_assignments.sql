{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH student_assignments AS (
    SELECT
        *
    FROM
        {{ref('stg_lms_student_assignments')}}
),

abpusers AS (
    SELECT 
        *
    FROM
        {{ref('stg_lms_abpusers')}}
),

course_assigned_students AS (
    SELECT 
        *
    FROM
        {{ ref('dim_course_assigned_students') }}
)

SELECT
    sa.*,
    user_last_name as creator_user_last_name,
    user_surname as creator_user_surname,
    username as creator_username,
    course_name,
    student_user_last_name,
    student_user_surname,
    student_username
FROM 
    student_assignments sa
    LEFT JOIN abpusers a 
    ON sa.creator_user_id = a.user_id
    LEFT JOIN course_assigned_students cas 
    ON cas.course_assigned_students_id = sa.course_assigned_student_id 