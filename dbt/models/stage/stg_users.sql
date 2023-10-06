{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS(
    SELECT
        "Id" :: INT AS "hrm_user_id",
        "job" :: INT AS "job_type",
        "usercode" :: VARCHAR AS "employee_code",
        "Name" :: VARCHAR AS "lastname",
        "major" :: VARCHAR AS "major",
        "phone" :: VARCHAR AS "phone",
        "branch" :: INT AS "branch",
        "status" :: INT AS "status",
        "surname" :: VARCHAR AS "surname",
        "isactive" :: BOOLEAN AS "is_active",
        "username" :: VARCHAR AS "username",
        "usertype" :: INT AS "employee_type",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "userlevel" :: INT AS "employee_level",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "emailaddress" :: VARCHAR AS "email"
    FROM
        {{ source(
            'raw_hrm',
            'hrm_abpusers'
        ) }}
    WHERE 
        isdeleted = false
),
tsh_abpuser AS (
    SELECT
        "Id" :: INT AS "tsh_user_id",
        "emailaddress" :: VARCHAR AS "email",
        "isactive" :: BOOLEAN AS "is_active",
        "isdeleted" :: BOOLEAN AS "is_deleted"
    FROM
        {{ source(
            'raw_timesheet',
            'tsh_abpusers'
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        source.*,
        tsh_abpuser.tsh_user_id
    FROM source
    INNER JOIN tsh_abpuser 
    ON tsh_abpuser.email = source.email
)
SELECT
    *
FROM
    data_type_rename_conversion
