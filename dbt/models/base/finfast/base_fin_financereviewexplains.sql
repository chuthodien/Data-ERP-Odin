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
	    'fin_financereviewexplains'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"tenantid" :: INT AS "tenant_id",
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"incomingusd" :: INT AS "incoming_usd",
	"incomingvnd" :: INT AS "incoming_vnd",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	"outcomingusd" :: INT AS "outcoming_usd",
	"outcomingvnd" :: INT AS "outcoming_vnd",
	"creatoruserid" :: INT AS "creatoruser_id",
	"deleteruserid" :: INT AS "deleteruser_id",
	"incomingdiffusd" :: INT AS "incomingdiff_usd",
	"incomingdiffvnd" :: INT AS "incomingdiff_vnd",	
	"outcomingdiffusd" :: INT AS "outcomingdiff_usd",
	"outcomingdiffvnd" :: INT AS "outcomingdiff_vnd",	
	"lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	"incomingdiffusdnote" :: VARCHAR AS "incomingdiff_usd_note",
	"incomingdiffvndnote" :: VARCHAR AS "incomingdiff_vnd_note",	
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
	"outcomingdiffusdnote" :: VARCHAR AS "outcomingdiff_usd_note",
	"outcomingdiffvndnote" :: VARCHAR AS "outcomingdiff_vnd_note",
	"incomingusdtransaction" :: INT AS "incoming_usd_transaction",
	"incomingvndtransaction" :: INT AS "incoming_vnd_transaction",	
	"outcomingusdtransaction" :: INT AS "outcoming_usd_transaction",
	"outcomingvndtransaction" :: INT AS "outcoming_vnd_transaction"
    FROM
	source
)
SELECT
    *
FROM
    data_type_rename_conversion
