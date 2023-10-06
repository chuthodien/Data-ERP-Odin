{{ config (
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_hrmv2',
            'hrm_employees'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_user_id",
        "sex" :: INT AS "sex",
        "email" :: VARCHAR AS "email",
        "phone" :: VARCHAR AS "phone",
        "avatar" :: VARCHAR AS "avatar",
        "bankid" :: INT AS "bank_id",
        "idcard" :: VARCHAR AS "idcard",
        "salary" :: INT AS "salary",
        "status" :: INT AS "status",
        "address" :: VARCHAR AS "address",
        "levelid" :: INT AS "employee_level",
        "taxcode" :: VARCHAR AS "taxcode",
        {{ to_timestamp("birthday") }} :: TIMESTAMP AS "birthday",
        "branchid" :: INT AS "branch",
        "fullname" :: VARCHAR AS "fullname",
        "issuedby" :: VARCHAR AS "issued_by",
        {{ to_timestamp("issuedon") }} :: TIMESTAMP AS "issued_on",
        "tenantid" :: INT AS "tenant_id",
        "usertype" :: INT AS "employee_type",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "realsalary" :: INT AS "real_salary",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "jobpositionid" :: INT AS "job_position_id",
        "remainleaveday" :: FLOAT AS "remain_leave_day",
        "insurancestatus" :: INT AS "insurance_status",
        "placeofpermanent" :: VARCHAR AS "place_of_permanent",
        {{ to_timestamp("startworkingdate") }} :: TIMESTAMP AS "start_working_time",
        "bankaccountnumber" :: VARCHAR AS "bank_account_number",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        "probationpercentage" :: INT AS "probation_percentage",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
