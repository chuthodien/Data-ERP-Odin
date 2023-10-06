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
            'lms_courses'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "lms_courses_id",
        "name" :: VARCHAR AS "name",
        "Type" :: INT AS "Type",
        "state" :: INT AS "state",
        "sourse" :: INT AS "sourse",
        "levelid" :: VARCHAR AS "level_id",
        "Version" :: VARCHAR AS "Version",
        "syllabus" :: VARCHAR AS "syllabus",
        "tenantid" :: INT AS "tenant_id", 
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "identifier" :: VARCHAR AS "identifier",
        "imagecover" :: VARCHAR AS "image_cover",
        "languageid" :: INT AS "language_id",
        "soursepath" :: VARCHAR AS "sourse_path",
        "description" :: VARCHAR AS "description",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "relatedimage" :: VARCHAR AS "related_image",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        "relatedinformation" :: VARCHAR AS "related_information",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "restrictstudentfromv__hiscourseafterenddate" :: BOOLEAN AS "restrictstudentfromv__hiscourseafterenddate",
        "studentcanonlypartic__oursebetweenthesedate" :: BOOLEAN AS "studentcanonlypartic__oursebetweenthesedate",
        "restrictstudentsfrom__iscoursebeforeenddate" :: BOOLEAN AS "restrictstudentsfrom__iscoursebeforeenddate"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
