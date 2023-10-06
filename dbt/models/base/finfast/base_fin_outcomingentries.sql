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
	    'fin_outcomingentries'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"Name" :: VARCHAR AS "name",
	"Value" :: FLOAT AS "value",
	"branchid" :: INT AS "branch_id",
	{{ to_timestamp("senttime") }} :: TIMESTAMP AS "senttime",
	"tenantid" :: INT AS "tenant_id",
	"accountid" :: INT AS "account_id",
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"currencyid" :: INT AS "currency_id",
	"paymentcode" :: VARCHAR AS "payment_code",
	{{ to_timestamp("approvedtime") }} :: TIMESTAMP AS "approvedtime",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	{{ to_timestamp("executedtime") }} :: TIMESTAMP AS "executedtime",
	"isacceptfile" :: INT AS "isacceptfile",
	"accreditation" :: BOOLEAN AS "accreditation",
	"creatoruserid" :: INT AS "creatoruser_id",
	"deleteruserid" :: INT AS "deleteruser_id",	
	"workflowstatusid" :: INT AS "workflow_status_id",	
	"lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
	"outcomingentrytypeid" :: INT AS "outcoming_entry_typeid"	
    FROM
	source
)
SELECT
    *
FROM
    data_type_rename_conversion
