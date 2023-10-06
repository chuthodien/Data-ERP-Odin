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
            'lms_userfollowqas'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "userfollowqas_id",
        "userid" :: INT AS "user_id",
        "followid" :: VARCHAR AS "follow_id",
        "tenantid" :: INT AS "tenant_id",
        "followtype" :: VARCHAR AS "follow_type"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
