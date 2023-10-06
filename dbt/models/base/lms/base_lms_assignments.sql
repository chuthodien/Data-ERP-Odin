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
            'lms_assignments'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "lms_assignment_id",
        "title" :: VARCHAR AS "title",
        "status" :: INT AS "status",
        "Content" AS "content",
        "courseid" :: VARCHAR AS "course_id",
        "tenantid" :: INT AS "tenant_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        "displaygrade" :: INT AS "display_grade",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "submissiontype" :: INT AS "submission_type",
        "isgroupassignment" :: BOOLEAN AS "is_group_assignment",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "isassignindividualgrade" :: BOOLEAN AS "is_assign_individual_grade"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
