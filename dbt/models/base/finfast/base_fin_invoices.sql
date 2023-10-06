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
	    'fin_invoices'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"itf" :: INT AS "itf",
	"ntf" :: INT AS "ntf",
	"note" :: VARCHAR AS "note",
	"Year" :: INT AS "Year",
	"Month" :: INT AS "Month",
	"status" :: INT AS "status",
	{{ to_timestamp("deadline") }} :: TIMESTAMP AS "dealine",
	"tenantid" :: INT AS "tenant_id",
	"accountid" :: INT AS "account_id",
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"currencyid" :: INT AS "currency_id",
	"nameinvoice" :: VARCHAR AS "nameinvoice",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	"creatoruserid" :: INT AS "creatoruser_id",
	"deleteruserid" :: INT AS "deleteruser_id",	
	"invoicenumber" :: VARCHAR AS "invoice_number",	
	"collectiondebt" :: INT AS "collectiondebt",	
	"lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
	source
)
SELECT
    *
FROM
    data_type_rename_conversion
