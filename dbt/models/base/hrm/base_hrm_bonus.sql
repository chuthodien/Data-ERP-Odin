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
            'hrm_bonus'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "bonus_id",
        "Name" :: VARCHAR AS "name",
        "requestid" :: INT AS "request_id",
        {{ to_timestamp(
            '"ngaychitra"'
        ) }} :: TIMESTAMP AS "payment_date",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp('dateexcuted') }} :: TIMESTAMP AS "excuted_date",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
