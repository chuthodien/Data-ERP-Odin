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
	    'fin_outcomingentrytypes'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"code" :: VARCHAR AS "code",
	"Name" :: VARCHAR AS "name",
	"Level" :: INT AS "level",
	"parentid" :: INT AS "parent_id",
	"pathname" :: VARCHAR AS "path_name",
	"tenantid" :: INT AS "tenant_id",	
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"workflowid" :: INT AS "workflow_id",	
	"expensetype" :: INT AS "expense_type",	
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
