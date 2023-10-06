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
            'lms_studentanswers'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "studentanswers_id",
        "mark" :: INT AS "mark",
        "status" :: INT AS "status",
        "answerid" :: VARCHAR AS "answer_id",
        "moduleid" :: VARCHAR AS "module_id",
        "tenantid" :: INT AS "tenant_id", 
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "answertext" :: VARCHAR AS "answertext",
        "questionid" :: VARCHAR AS "questionid",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "testattempid" :: VARCHAR AS "testattempid",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "questionversion" :: VARCHAR AS "questionversion",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
