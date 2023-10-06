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
            'lms_usergroups'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "usergroups_id",
        "userid" :: INT AS "user_id",
        "groupid" :: VARCHAR AS "group_id",
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
