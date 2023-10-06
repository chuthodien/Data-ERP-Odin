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
            'lms_usercertifications'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "usercertifications_id",
        "point" :: INT AS "point",
        "tenantid" :: INT AS "tenant_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "templateid" :: VARCHAR AS "template_id",
        "totalpoint" :: INT AS "total_point",       
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "certification" :: VARCHAR AS "certification",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "graduatedlevel" :: VARCHAR AS "graduatedlevel",
        "courseinstanceid" :: VARCHAR AS "course_instance_id",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "courseassignedstudentid" :: VARCHAR AS "course_assigned_student_id"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
