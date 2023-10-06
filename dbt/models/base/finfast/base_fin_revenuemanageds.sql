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
	    'fin_revenuemanageds'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"note" :: VARCHAR AS "note",	
	"Month" :: INT AS "month",	
	"status" :: INT AS "status",	
	"unitid" :: INT AS "unit_id",	
	{{ to_timestamp("deadline") }} :: TIMESTAMP AS "deadline",
	"tenantid" :: INT AS "tenant_id",	
	"accountid" :: INT AS "account_id",	
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"nameinvoice" :: VARCHAR AS "name_invoice",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	"debtreceived" :: INT AS "debtreceived",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	"remindstatus" :: INT AS "remind_status",
	"creatoruserid" :: INT AS "creatoruser_id",
	"deleteruserid" :: INT AS "deleteruser_id",
	"collectiondebt" :: INT AS "collection_debt",
	{{ to_timestamp("sendinvoicedate") }} :: TIMESTAMP AS "send_invoice_date",						
	"lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
	source
)
SELECT
    *
FROM
    data_type_rename_conversion
