{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *,
        "Date" ::VARCHAR AS "date"
    FROM
        {{ source(
            'raw_hrmv2',
            'hrm_punishments'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_punishments_id",
         {{ to_timestamp("date") }} :: TIMESTAMP AS "date",
        "Name" :: VARCHAR AS "name",
        "isactive" :: BOOLEAN AS "is_active",
        "tenantid" :: INT AS "tenantid",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteuser_id",
        "punishmenttypeid" :: INT AS "punishmenttype_id",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
