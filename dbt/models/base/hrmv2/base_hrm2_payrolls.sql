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
            'hrm_payrolls'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_payrolls_id",
        "status" :: INT AS "status",
        "opentalk" :: INT AS "opentalk",
        "tenantid" :: INT AS "tenantid",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("applymonth") }} :: TIMESTAMP AS "apply_month",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteuser_id",
        "normalworkingday" :: INT AS "normal_working_day",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
