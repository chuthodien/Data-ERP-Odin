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
            'lms_groupassingedquizzes'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "groupassingedquizzes_id",
        "tenantid" :: INT AS "tenant_id",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        "coursegroupid" :: VARCHAR AS "course_group_id",
        "creatoruserid" :: INT AS "creator_user_id",
        "quizsettingid" :: VARCHAR AS "quizsettingid"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
