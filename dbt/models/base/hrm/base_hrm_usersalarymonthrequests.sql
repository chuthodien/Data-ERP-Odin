{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_hrm',
            'hrm_usersalarymonthrequests'
        ) }}
    WHERE
        isdeleted = false
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_usersalarymonthrequest_id",
        "Year" :: INT AS "year",
        "Month" :: INT AS "month",
        "bonus",
        "loans",
        "branch",
        "salary",
        "status",
        "userid" :: INT AS "hrm_user_id",
        "usertype" :: INT AS "hrm_user_type",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "requestid" :: INT AS "request_id",
        "userlevel" :: INT AS "user_level",
        "punishment" :: INT AS "punishment",
        "realsalary" :: INT AS "real_salary",
        "leavedayplus" :: FLOAT AS "leave_day_plus",
        "leavedayminus" :: FLOAT AS "leave_day_minus",
        "normalsalary" :: INT AS "normal_salary",
        "remuneration" :: INT AS "remuneration",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "normalworking" :: FLOAT AS "normal_working",
        "childcompanyid" :: INT AS "child_company_id",
        "contractsalary" :: INT AS "contract_salary",
        "overtimesalary" :: INT AS "overtime_slary",
        "absencedaymonth" :: FLOAT AS "absence_day_month",
        "absencedaystring" :: VARCHAR AS "absence_day_string",
        "allowedleaveday" :: FLOAT AS "allowed_leave_day",
        "allowedleavedayold" :: FLOAT AS "allowed_leave_day_old",
        "companynormalworking" :: INT AS "company_normal_working",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
ORDER BY year,month
