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
            'raw_buckets'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "key" :: INT AS "tracker_bucket_key",
        "Id" :: VARCHAR AS "tracker_bucket_id",
        {{ to_timestamp("created") }} :: TIMESTAMP AS "created",
        "name" :: VARCHAR AS "name",
        "type" :: VARCHAR AS "type",
        "client" :: VARCHAR AS "client",
        "hostname" :: VARCHAR AS "hostname"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
