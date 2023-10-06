{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *,
        "Date" :: VARCHAR AS "date"
    FROM
        {{ source(
            'raw_hrmv2',
            'hrm_debtpaids'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_debtpaids_id",
        {{ to_timestamp("date") }} :: TIMESTAMP AS "date",
        "note" :: VARCHAR AS "color",
        "money" :: INT AS "money",
        "debtid" :: INT AS "debt_id",
        "tenantid" :: INT AS "tenant_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
	    "paymenttype" :: INT AS "payment_type",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteruser_id",
        "payslipdetailid" :: INT AS "payslipdetail_id",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
