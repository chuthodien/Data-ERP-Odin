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
	    'fin_outcomingentrydetails'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"Name" :: VARCHAR AS "name",
	"total" :: INT AS "total",
	"branchid" :: INT AS "branch_id",
	"quantity" :: INT AS "quantity",
	"tenantid" :: INT AS "tenant_id",
	"accountid" :: INT AS "account_id",
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"unitprice" :: INT AS "unit_price",
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
