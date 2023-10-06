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
            'lms_usertimezones'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "usertimezones_id",
        "tenantid" :: INT AS "tenant_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "timezoneid" :: VARCHAR AS "timezone_id",
        "displayname" :: VARCHAR AS "display_name",     
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "standardname" :: VARCHAR AS "standard_name",
        "baseutcoffset" :: VARCHAR AS "BaseUtcOffset",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "supportsdaylightsavingtime" :: BOOLEAN AS "supports_day_ligh_saving_time"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
