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
            'raw_users'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tracker_user_id",
        "device_id" :: VARCHAR AS "tracker_user_device_id",
        "name" :: VARCHAR AS "name",
        "email" :: VARCHAR AS "email",
        "access_token" :: VARCHAR AS "access_token",
        "refresh_token" :: VARCHAR AS "refresh_token",
        {{ to_timestamp("last_used_at") }} :: TIMESTAMP AS "last_used_at"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
