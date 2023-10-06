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
	    'fin_abpusers'
	) }}
),
data_type_rename_conversion AS (
    SELECT
	"Id" :: INT AS "finfast_id",
	"Name" :: VARCHAR AS "name",
	"surname" :: VARCHAR AS "surname",
	"isactive" :: BOOLEAN AS "is_active",
	"Password" :: VARCHAR AS "password",
	"tenantid" :: INT AS "tenant_id",
	"username" :: VARCHAR AS "user_name",
	"isdeleted" :: BOOLEAN AS "is_deleted",
	"komuuserid" :: INT AS "komu_user_id",
	"phonenumber" :: VARCHAR AS "phone_number",
	{{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
	{{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
	"emailaddress" :: VARCHAR AS "email_address",
	"komuusername" :: VARCHAR AS "komuuser_name",
	"creatoruserid" :: INT AS "creatoruser_id",
	"deleteruserid" :: INT AS "deleteruser_id",
	"securitystamp" :: VARCHAR AS "security_stamp",
	"concurrencystamp" :: VARCHAR AS "concurrency_stamp",
	"isemailconfirmed" :: BOOLEAN AS "is_email_comfirmed",
	"islockoutenabled" :: BOOLEAN AS "is_lockout_enabled",
	"accessfailedcount" :: INT AS "access_failed_count",
	{{ to_timestamp("lockoutenddateutc") }} :: TIMESTAMP AS "lockout_end_date_utc",
	"passwordresetcode" :: VARCHAR AS "password_reset_code",
	"istwofactorenabled" :: BOOLEAN AS "is_two_factor_enabled",
	"lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	"normalizedusername" :: VARCHAR AS "normalized_username",
	"authenticationsource" :: VARCHAR AS "authentication_source",
	{{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
	"emailconfirmationcode" :: VARCHAR AS "email_confirmation_code",
	"isphonenumberconfirmed" :: BOOLEAN AS "is_phonenumber_confirmed",
	"normalizedemailaddress" :: VARCHAR AS "normalized_emailaddress"

    FROM
	source
)
SELECT
    *
FROM
    data_type_rename_conversion
