{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_lms',
            'lms_studentassignments'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "studentassignments_id",
        "point" :: INT AS "point",
        "tenantid" :: INT AS "tenantid",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        "creatoruserid" :: INT AS "creator_user_id",
        "assignmentsettingid" :: VARCHAR AS "assignment_setting_id",
        "courseassignedstudentid" :: VARCHAR AS "course_assigned_student_id"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
