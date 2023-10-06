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
            'tsh_customers'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_customers",
        "Name" :: VARCHAR AS "name",
        "address" :: VARCHAR AS "address",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp('creationtime') }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp('deletiontime') }} :: TIMESTAMP AS "deletion_time",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
