{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
	*
    FROM
	{{ source(
	    'raw_finfast',
	    'fin_btransactionlogs'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"Key" :: VARCHAR AS "key",
	{{ to_timestamp("timeat") }} :: TIMESTAMP AS "time_at",
	"isvalid" :: BOOLEAN AS "is_valid",
	"message" :: VARCHAR AS "message",
	"tenantid" :: INT AS "tenant_id",
	"isdeleted" :: BOOLEAN AS "is_deleted",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	"errormessage" :: VARCHAR AS "error_message",
	"creatoruserid" :: INT AS "creatoruser_id",
	"deleteruserid" :: INT AS "deleteruser_id",
	"btransactionid" :: INT AS "btransaction_id",
	"lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
	source
)
SELECT
    *
FROM
    data_type_rename_conversion
