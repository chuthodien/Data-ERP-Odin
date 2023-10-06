{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_lms',
            'lms_userlinks'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "userlinks_id",
        "link" :: VARCHAR AS "link",
        "title" :: VARCHAR AS "title",
        "tenantid" :: INT AS "tenant_id",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        "creatoruserid" :: INT AS "creator_user_id"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
