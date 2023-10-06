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
            'hrm_abpusers'
        ) }}
),
data_selection AS (
    SELECT
        *
    FROM
        source
    WHERE
        emailaddress LIKE '%@ncc.asia'
        AND usercode IS NOT NULL
        AND isdeleted IS FALSE
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_user_id",
        {{ to_timestamp("dob") }} AS "date_of_birth",
        "job" :: INT AS "job_type",
        "sex" :: INT AS "sex",
        "bank" :: VARCHAR AS "bank",
        "usercode" :: VARCHAR AS "employee_code",
        "Name" :: VARCHAR AS "lastname",
        "major" :: VARCHAR AS "major",
        "phone" :: VARCHAR AS "phone",
        "branch" :: INT AS "branch",
        "idcard" :: VARCHAR AS "idcard",
        "school" :: VARCHAR AS "school",
        "status" :: INT AS "status",
        "address" :: VARCHAR AS "address",
        "surname" :: VARCHAR AS "surname",
        "taxcode" :: VARCHAR AS "tax_code",
        "isactive" :: BOOLEAN AS "is_active",
        "issuedby" :: VARCHAR AS "issued_by",
        {{ to_timestamp("issuedon") }} AS "issued_on",
        "username" :: VARCHAR AS "username",
        "usertype" :: INT AS "employee_type",
        {{ to_timestamp("enddayoff") }} :: VARCHAR AS "end_day_off",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        "userlevel" :: INT AS "employee_level",
        "bankbranch" :: VARCHAR AS "bank_branch",
        "personemail" :: VARCHAR AS "personal_email",
        "phonenumber" :: VARCHAR AS "personal_phone",
        {{ to_timestamp("startdayoff") }} :: TIMESTAMP AS "start_day_off",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "emailaddress" :: VARCHAR AS "email",
        "isfullsalary" :: BOOLEAN AS "is_full_salary",
        "accountnumber" :: VARCHAR AS "account_number",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "childcompanyid" :: INT AS "child_company_id",
        "educationlevel" :: VARCHAR AS "education_level",
        "workinghistory" :: VARCHAR AS "working_history",
        "allowedleaveday" :: FLOAT AS "allowed_leave_day",
        "healthinsurance" :: VARCHAR AS "health_insurance",
        "ispaymentdayoff" :: BOOLEAN AS "is_payment_day_off",
        "socialinsurance" :: VARCHAR AS "social_insurance",
        "descriptondayoff" :: VARCHAR AS "descripton_day_off",
        "isemailconfirmed" :: BOOLEAN AS "is_email_confirmed",
        "islockoutenabled" :: BOOLEAN AS "is_lockout_enabled",
        "ownerbankaccount" :: VARCHAR AS "owner_bank_account",
        "placeofpermanent" :: VARCHAR AS "place_of_permanent",
        {{ calc_start_working_time() }} :: TIMESTAMP AS "start_working_time",
        "accessfailedcount" :: INT AS "access_failed_count",
        {{ to_timestamp("lockoutenddateutc") }} :: TIMESTAMP AS "lockou_tend_date_utc",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        "normalizedusername" :: VARCHAR AS "normalized_username",
        "authenticationsource" :: VARCHAR AS "authentication_source",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
        {{ to_timestamp("probationarystarttime") }} :: TIMESTAMP AS "probationary_start_time",
        "isphonenumberconfirmed" :: BOOLEAN AS "is_phonenumber_confirmed",
        "normalizedemailaddress" :: VARCHAR AS "normalized_emailaddress"
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
