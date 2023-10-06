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
            'hrm_debts'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_debts_id",
        "note" :: VARCHAR AS "note",
        "money" :: INT AS "money",
        "status" :: INT AS "status",
	    {{ to_timestamp("enddate") }} :: TIMESTAMP AS "end_date",
        "tenantid" :: INT AS "tenant_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
	    {{ to_timestamp("startdate") }} :: TIMESTAMP AS "start_date",
	    "employeeid" :: INT AS "employee_id",
	    "paymenttype" :: INT AS "payment_type",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
         "interestrate" :: INT AS "interestrate",
         "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteruser_id",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
