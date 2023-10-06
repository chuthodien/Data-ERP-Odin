{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_talent',
            'tal_abpusers'
        ) }}
),
data_selection AS(
    SELECT
        *
    FROM
        source
    WHERE
        username LIKE '%@%'
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_abpuser_id",
        "Name" :: VARCHAR AS "lastname",
        "address" :: VARCHAR AS "address",
        "surname" :: VARCHAR AS "surname",
        "branchid" :: INT AS "branch",
        "isactive" :: INT AS "is_active",
        "Password" :: VARCHAR AS "password",
        "tenantid" :: INT AS "tenant_id",
        "username" :: VARCHAR AS "username",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "avatarpath" :: VARCHAR AS "avatar_path",
        "positionid" :: INT AS "position_id",
        "phonenumber" :: VARCHAR AS "phone_number",
        "certificates",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "emailaddress" :: VARCHAR AS "email",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "securitystamp" :: VARCHAR AS "security_stamp",
        "concurrencystamp" :: VARCHAR AS "concurrency_stamp",
        "isemailconfirmed" :: BOOLEAN AS "is_email_confirmed",
        "islockoutenabled" :: BOOLEAN AS "is_lockout_enabled",
        signaturecontact :: VARCHAR,
        "accessfailedcount" :: INT AS "access_failed_count",
        {{ to_timestamp("lockoutenddateutc") }} :: TIMESTAMP AS "lockout_end_date_utc",
        "passwordresetcode" :: VARCHAR AS "password_reset_code",
        "personalattribute" :: VARCHAR AS "personal_attribute",
        "istwofactorenabled" :: BOOLEAN AS "is_two_factor_enabled",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        "normalizedusername" :: VARCHAR AS "normalized_username",
        "authenticationsource" :: VARCHAR AS "authentication_source",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "isphonenumberconfirmed" :: BOOLEAN AS "is_phonenumber_confirmed",
        "emailconfirmationcode" :: VARCHAR AS "email_confirmation_code",
        "normalizedemailaddress" :: VARCHAR AS "normalized_emailaddress"
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
