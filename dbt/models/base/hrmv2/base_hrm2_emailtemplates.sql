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
            'hrm_emailtemplates'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_emailtemplates_id",
        "ccs" :: VARCHAR AS "ccs",
        "Name" :: VARCHAR AS "name",
        "Type" :: INT AS "type",
        "subject" :: VARCHAR AS "subject",
        "tenantid" :: INT AS "tenant_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
	    "bodymessage" :: VARCHAR AS "body_message",
	    "description" :: VARCHAR AS "description",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	    "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteruser_id",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
