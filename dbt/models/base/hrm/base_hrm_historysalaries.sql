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
            'hrm_historysalaries'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "hrm_historysalaries_id",
        "amount" :: FLOAT AS "amount",
        "userid" :: INT AS "hrm_user_id",
        "requestid" :: INT AS "hrm_request_id",
        {{ to_timestamp("enddate") }} :: TIMESTAMP AS "end_date",
        {{ to_timestamp("startdate") }} :: TIMESTAMP AS "start_date",
        "description" :: text AS "description",
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
