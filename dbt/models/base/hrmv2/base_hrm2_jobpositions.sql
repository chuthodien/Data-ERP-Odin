{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_hrmv2',
            'hrm_jobpositions'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_jobpositions_id",
        "code" :: VARCHAR AS "code",
        "Name" :: VARCHAR AS "name",
        "color" :: VARCHAR AS "color",
        "tenantid" :: INT AS "tenantid",
        "isdeleted" :: BOOLEAN AS "is_deleted",
	    "shortname" :: VARCHAR AS "shortname",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteuser_id",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
