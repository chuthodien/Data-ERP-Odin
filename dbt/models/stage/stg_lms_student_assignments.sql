{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_lms_studentassignments'
        ) }}
),
data_mapped AS (
    SELECT
        studentassignments_id,
        point as score,
        tenantid as tenant_id,
        creation_time,
        creator_user_id,
        assignment_setting_id,
        course_assigned_student_id
    FROM
        source
)
SELECT
    *
FROM
    data_mapped 
