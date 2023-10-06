{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_timesheet',
            'tsh_abpusers'
        ) }}
    WHERE isdeleted is FALSE
),
data_selection AS (
    SELECT
        *
    FROM
        source
    WHERE
        emailaddress LIKE '%@ncc.asia'
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_user_id",
        "Name" :: VARCHAR AS "lastname",
        "Type" :: INT AS "employee_type",
        "Level" :: INT AS "employee_level",
        "branchold" :: INT AS "branch",
        "salary" :: FLOAT AS "salary",
        "address" :: VARCHAR "address",
        "surname" :: VARCHAR AS "firstname",
        "isactive" :: BOOLEAN AS "is_active",
        "jobtitle" :: VARCHAR AS "job_title",
        "emailaddress" :: VARCHAR AS "email",
        {{ to_timestamp("salaryat") }} :: TIMESTAMP AS "salary_at",
        "usercode" :: VARCHAR AS "employee_code",
        "username" :: VARCHAR AS "username",
        "managerid" :: INT AS "tsh_manager_user_id",
        "avatarpath" :: VARCHAR AS "tsh_avatar",
        "isstopwork" :: BOOLEAN AS "is_to_work",
        "phonenumber" :: VARCHAR AS "phonenumber",
        {{ to_timestamp("startdateat") }} :: TIMESTAMP AS "start_date_at",
        {{ to_timestamp(
            "morningendat",
            "HH24:MI"
        ) }} :: TIME AS "morning_end_at",
        {{ to_timestamp(
            "afternoonendat",
            "HH24:MI"
        ) }} :: TIME AS "afternoon_end_at",
        {{ to_timestamp(
            "morningstartat",
            "HH24:MI"
        ) }} :: TIME AS "morning_start_at",
        {{ to_timestamp(
            "afternoonstartat",
            "HH24:MI"
        ) }} :: TIME AS "afternoon_start_at",
        {{ hour_to_ms("morningworking") }} :: INT AS "morning_working_time",
        {{ hour_to_ms("afternoonworking") }} :: INT AS "afternoon_working",
        "isworkingtimedefault" :: BOOLEAN AS "is_working_time_default",
        "isdeleted" :: VARCHAR AS "is_deleted",
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
