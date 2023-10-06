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
            'tsh_absencedayrequests'
        ) }}

), data_type_rename_conversion AS (
    SELECT
        "Id"::INT AS "tsh_absencedayrequests_id",
        "userid"::INT AS "tsh_user_id",
        "Type"::INT AS "absence_type",
        "status"::INT AS "absence_status",
        "reason"::VARCHAR AS "reason",
        "dayofftypeid"::INT AS "day_off_type",
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

