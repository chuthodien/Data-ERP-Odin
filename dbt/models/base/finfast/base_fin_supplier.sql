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
	    'fin_supplier'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "id",
	"Name" :: VARCHAR AS "name",	
	"address" :: VARCHAR AS "address",	
	"tenantid" :: INT AS "tenant_id",	
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"taxnumber" :: VARCHAR AS "taxnumber",	
	"phonenumber" :: VARCHAR AS "phone_number",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	"creatoruserid" :: INT AS "creatoruser_id",
	"deleteruserid" :: INT AS "deleteruser_id",
	"contactpersonname" :: VARCHAR AS "contact_person_name",
	"contactpersonphone" :: VARCHAR AS "contact_person_phone",
	"lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
	source
)
SELECT
    *
FROM
    data_type_rename_conversion
