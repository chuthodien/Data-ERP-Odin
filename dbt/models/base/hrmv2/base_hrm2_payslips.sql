{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_hrmv2',
            'hrm_payslips'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_payslips_id",
        "bankid" :: INT AS "bank_id",
	"othour" :: INT AS "othour",
	"offday" :: FLOAT AS "off_day",
	"salary" :: INT AS "salary",
	"levelid" :: INT AS "level_id",
	"bankname" :: VARCHAR AS "bank_name",
	"branchid" :: INT AS "branch_id",
	"tenantid" :: INT AS "tenantid",
	"usertype" :: INT AS "user_type",
        "isdeleted" :: BOOLEAN AS "is_deleted",
	"normalday" :: FLOAT AS "normal_day",
	"payrollid" :: INT AS "payroll_id",
	"employeeid" :: INT AS "employee_id",
	"complainnote" :: VARCHAR AS "complain_note",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "addedleaveday" :: INT AS "added_leave_day",
        "confirmstatus" :: INT AS "confirm_status",
	    "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteruser_id",
        "jobpositionid" :: INT AS "jobposition_id",
        "opentalkcount" :: INT AS "opentalk_count",
        "refundleaveday" :: FLOAT AS "refund_leaveday",
        "complaindeadline" :: VARCHAR AS "complain_deadline",
        "bankaccountnumber" :: VARCHAR AS "bank_account_number",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
	    "remainleavedayafter" :: FLOAT AS "remain_leaveday_after",
	    {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time",
	    "remainleavedaybefore" :: FLOAT AS "remain_leaveday_before",
	    "workatofficeoronsiteday" :: INT AS "workatofficeoronsiteday"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
