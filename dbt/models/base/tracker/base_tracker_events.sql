{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_tracker',
            'raw_events'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tracker_event_id",
        "bucket_id" :: INT AS "bucket_id",
        {{ to_timestamp("timestamp") }} :: TIMESTAMP AS "timestamp",
        "duration" :: INTERVAL AS "duration",
        "datastr" :: text AS "datastr"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
