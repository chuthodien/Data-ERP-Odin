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
            'tsh_absencedaydetails'
        ) }}

), data_type_rename_conversion AS (
    SELECT
        "Id"::INT AS "tsh_absencedaydetails_id",
        "Hour"::FLOAT AS "hour",
        {{ to_timestamp("dateat") }} :: TIMESTAMP AS "date_at",
        "datetype"::INT AS "date_type",
        "requestid"::INT AS "tsh_request_id",
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

