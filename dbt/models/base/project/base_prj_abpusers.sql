{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_project',
            'prj_abpusers'
        ) }}
),
data_selection AS (
    SELECT
        *
    FROM
        source
    WHERE
        emailaddress LIKE '%@ncc%'
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "prj_user_id",
        {{ to_timestamp("dob") }} AS "date_of_birth",
        "job" :: INT AS "job_type",
        "Name" :: VARCHAR AS "lastname",
        "branchid" :: INT AS "branch",
        "surname" :: VARCHAR AS "surname",
        "isactive" :: BOOLEAN AS "is_active",
        "poolnote" :: VARCHAR AS "pool_note",
        "starrate" :: FLOAT AS "star_rate",
        "usercode" :: VARCHAR AS "usercode",
        "username" :: VARCHAR AS "username",
        "usertype" :: INT AS "employee_type",
        "userlevel" :: INT AS "employee_level",
        "avatarpath" :: VARCHAR AS "avatar_path",
        "komuuserid" :: VARCHAR AS "komu_user_id",
        "phonenumber" :: VARCHAR AS "personal_phone",
        "emailaddress" :: VARCHAR AS "email",
        "isemailconfirmed" :: BOOLEAN AS "is_email_confirmed",
        "islockoutenabled" :: BOOLEAN AS "is_lockout_enabled",
        "accessfailedcount" :: INT AS "access_failed_count",
        {{ to_timestamp("lockoutenddateutc") }} :: TIMESTAMP AS "lockou_tend_date_utc",
        "istwofactorenabled" :: BOOLEAN AS "is_two_factor_enabled",
        "normalizedusername" :: VARCHAR AS "normalized_username",
        "authenticationsource" :: VARCHAR AS "authentication_source",
        "isphonenumberconfirmed" :: BOOLEAN AS "is_phonenumber_confirmed",
        "normalizedemailaddress" :: VARCHAR AS "normalized_emailaddress",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
