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
            'hrm_salaryexecutions'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_salaryexecutions_id",
        "Year" :: INT AS "year",
        "Month" :: INT AS "month",
        "salary" :: FLOAT AS "salary",
        "userid" :: INT AS "hrm_user_id",
        {{ to_timestamp("enddate") }} :: TIMESTAMP AS "end_date",
        {{ to_timestamp("startdate") }} :: TIMESTAMP AS "start_date",
        "isdeleted" :: BOOLEAN AS "is_deleted",
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
