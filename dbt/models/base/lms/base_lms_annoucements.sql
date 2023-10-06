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
            'lms_annoucements'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "lms_annoucements_id",
        "title" :: VARCHAR AS "title",
        "Content" :: VARCHAR AS "content",
        "tenantid" :: INT AS "tenant_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        "creatoruserid" :: INT AS "creatoruserid",
        "deleteruserid" :: INT AS "deleter_user_id",
        "courseinstanceid" :: VARCHAR AS "courseinstanceid",
        "lastmodifieruserid" :: INT AS "lastmodifieruserid",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "lastmodificationtime"      
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
