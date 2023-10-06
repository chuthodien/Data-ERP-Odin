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
	    'fin_bankaccounts'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "bank_account_id",
	"amount" :: INT AS "amount",
	"bankid" :: INT AS "bankid",
	"tenantid" :: INT AS "tenant_id",
	"accountid" :: INT AS "account_id",
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"banknumber" :: VARCHAR AS "bank_number",
	"currencyid" :: INT AS "currency_id",
	"holdername" :: VARCHAR AS "holder_name",
	"basebalance" :: INT AS "basebalance",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	"lockedstatus" :: BOOLEAN AS "locked_status",
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

