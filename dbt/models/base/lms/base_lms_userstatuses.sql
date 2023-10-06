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
            'lms_userstatuses'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "userstatuses_id",
        "Level" :: INT AS "level",
        "isstatic" :: BOOLEAN AS "isstatic",
        "tenantid" :: INT AS "tenant_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "identifier" :: VARCHAR AS "identifier",
        "displayname" :: VARCHAR AS "display_name",     
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "requirednumber" :: INT AS "required_number",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        "lowcompareoperation" :: INT AS "low_compare_operation",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
