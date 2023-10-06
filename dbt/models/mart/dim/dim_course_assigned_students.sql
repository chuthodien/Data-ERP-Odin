{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH course_assigned_students AS (
    SELECT 
        *
    FROM
        {{ ref('stg_lms_course_assigned_students') }}
),

courses AS (
    SELECT 
        *
    FROM
        {{ ref('dim_courses') }}
),

abpusers AS (
    SELECT 
        *
    FROM
        {{ ref('stg_lms_abpusers') }}
)

SELECT 
    cas.*,
    course_name,
    user_last_name as student_user_last_name,
    user_surname as student_user_surname,
    username as student_username
FROM
    course_assigned_students cas
    LEFT JOIN courses c
    ON cas.course_instance_id = c.course_instances_id
    LEFT JOIN abpusers a 
    ON cas.student_id =  a.user_id    