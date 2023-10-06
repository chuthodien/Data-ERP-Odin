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
	    'fin_outcomingentryhistories'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"note" :: VARCHAR AS "note",
	"tovalue" :: INT AS "to_value",
	"tenantid" :: INT AS "tenant_id",
	"fromvalue" :: INT AS "from_value",
	"isdeleted" :: BOOLEAN AS "is_deleted",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	"creatoruserid" :: INT AS "creatoruser_id",
	"deleteruserid" :: INT AS "deleteruser_id",	
	"outcomingentryid" :: INT AS "outcoming_entry_id",		
	"lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
	source
)
SELECT
    *
FROM
    data_type_rename_conversion
