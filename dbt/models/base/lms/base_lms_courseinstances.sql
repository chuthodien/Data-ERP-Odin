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
            'lms_courseinstances'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "courseinstances_id",
        "status" :: INT AS "status", 
        {{ to_timestamp("endtime") }} :: TIMESTAMP AS "end_time",
        "Version" :: VARCHAR AS "version",
        "courseid" :: VARCHAR AS "course_id",
        "tenantid" :: INT AS "tenant_id", 
        "allowskip" :: BOOLEAN AS "allowskip",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("starttime") }} :: TIMESTAMP AS "start_time",
        "totalquiz" :: INT AS "totalquiz",
        "passingmark" :: INT AS "passingmark",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "numberdaytostudy" :: INT AS "numberdaytostudy",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        "allowfinalquizretry" :: BOOLEAN AS "allowfinalquizretry",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "enablecoursegradingscheme" :: BOOLEAN AS "enablecoursegradingscheme"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
