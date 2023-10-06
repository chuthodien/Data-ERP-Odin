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
            'tsh_projects'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_project_id",
        "code" :: VARCHAR AS "tsh_project_code",
        "Name" :: VARCHAR AS "project_name",
        "note" :: VARCHAR AS "project_note",
        "status" :: INT AS "tsh_project_status",
        {{ to_timestamp('timeend') }} :: TIMESTAMP "end_end",
        {{ to_timestamp('timestart') }} :: TIMESTAMP "start_time",
        "customerid" :: INT AS "tsh_customer_id",
        "projecttype" :: INT AS "tsh_project_type",
        "isalluserbelongto" :: BOOLEAN AS "is_general",
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
