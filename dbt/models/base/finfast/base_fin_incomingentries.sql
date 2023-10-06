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
	    'fin_incomingentries'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"Name" :: VARCHAR AS "name",
	"Value" :: BIGINT AS "value",
	"status" :: BOOLEAN AS "status",
	"branchid" :: INT AS "branch_id",
	"tenantid" :: INT AS "tenant_id",
	"accountid" :: INT AS "account_id",
	"invoiceid" :: INT AS "invoice_id",
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"currencyid" :: INT AS "currency_id",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	"exchangerate" :: INT AS "exchangerate",
	"tocurrencyid" :: INT AS "to_currency_id",
	"creatoruserid" :: INT AS "creatoruser_id",
	"deleteruserid" :: INT AS "deleteruser_id",
	"btransactionid" :: INT AS "btransaction_id",
	"banktransactionid" :: INT AS "bank_transaction_id",	
	"lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	"incomingentrytypeid" :: INT AS "incoming_entry_typeid",	
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
	source
)
SELECT
    *
FROM
    data_type_rename_conversion
