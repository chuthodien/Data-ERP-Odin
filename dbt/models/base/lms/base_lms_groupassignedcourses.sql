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
            'lms_groupassignedcourses'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "groupassignedcourses_id",
        "groupid" :: VARCHAR AS "groupid",
        "tenantid" :: INT AS "tenant_id",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        "creatoruserid" :: INT AS "creator_user_id",
        "courseinstanceid" :: VARCHAR AS "courseinstanceid"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
