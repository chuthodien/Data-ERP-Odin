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
            'lms_courselevels'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "courselevels_id",
        "Level" :: INT AS "level",
        "isstatic" :: BOOLEAN AS "isstatic",
        "tenantid" :: INT AS "tenant_id", 
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "identifier" :: VARCHAR AS "identifier",
        "displayname" :: VARCHAR AS "displayname",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        "lowcompareoperation" :: INT AS "low_compare_operation",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "requiredstudentlevel" :: INT AS "required_student_level"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
