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
            'tsh_tasks'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_task_id",
        "Name" :: VARCHAR AS "task_name",
        "Type" :: INT AS "task_type",
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
