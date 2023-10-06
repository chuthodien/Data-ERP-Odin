{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_lms',
            'lms_abpusers'
        ) }}
),

data_selection AS (
    SELECT
        *
    FROM
        source
),

data_type_rename_conversion AS (
    SELECT
        "id" :: INT AS "lms_user_id",
        "name" :: VARCHAR AS "name",
        "title" :: VARCHAR AS "title",
        "avatar" :: VARCHAR AS "avatar",
        "surname" :: VARCHAR AS "surname",
        "isactive" :: BOOLEAN AS "is_active",
        "Password" :: VARCHAR AS "password", 
        "statusid" :: VARCHAR AS "status_id",
        "tenantid" :: INT AS "tenant_id",
        "username" :: VARCHAR AS "username",
        "biography" :: VARCHAR AS "biography",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "languageid" :: INT AS "language_id",
        "timezoneid" :: VARCHAR AS "timezone_id",
        "displayname" :: VARCHAR AS "displayname",
        "phonenumber" :: VARCHAR AS "phone_number",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "emailaddress" :: VARCHAR AS "email_address",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        {{ to_timestamp("lastlogintime") }} :: TIMESTAMP AS "last_login_time",
        "securitystamp" :: VARCHAR AS "security_stamp",
        "concurrencystamp" :: VARCHAR AS "concurrency_stamp",
        "isemailconfirmed" :: BOOLEAN AS "is_email_confirmed",
        "islockoutenabled" :: BOOLEAN AS "is_lockout_enabled",
        "accessfailedcount" :: INT AS "access_failed_count",
        "lockoutenddateutc" :: VARCHAR AS "lockout_end_date_utc",
        "passwordresetcode" :: VARCHAR AS "password_reset_code",
        "istwofactorenabled" :: BOOLEAN AS "is_two_factor_enabled",
        "lastmodifieruserid" :: INT AS "last_modifier_userid",
        "normalizedusername" :: VARCHAR AS "normalized_username",
        "authenticationsource" :: VARCHAR AS "authentication_source",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "emailconfirmationcode" :: VARCHAR AS "email_confirmation_code",
        "isphonenumberconfirmed" :: BOOLEAN AS "is_phonenumber_confirmed",
        "normalizedemailaddress" :: VARCHAR AS "normalized_email_address",
        "userpersonalinfoviewbypublic" :: BOOLEAN AS "user_personal_info_view_by_public",
        "userpersonallinksviewbypublic" :: BOOLEAN AS "user_personal_links_view_by_public",
        "userpersonalachievementviewbypublic" :: BOOLEAN AS "user_personal_achievement_view_by_public",
        "userpersonalcertificationviewbypublic" :: BOOLEAN AS "userpersonalcertificationviewbypublic"
    FROM
        data_selection
)

SELECT
    *
FROM
    data_type_rename_conversion
