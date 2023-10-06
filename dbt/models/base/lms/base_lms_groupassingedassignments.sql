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
            'lms_groupassingedassignments'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "groupassingedassignments_id",
        "tenantid" :: INT AS "tenant_id",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        "coursegroupid" :: VARCHAR AS "course_group_id",
        "creatoruserid" :: INT AS "creator_user_id",
        "assignmentsettingid" :: VARCHAR AS "assignment_setting_id"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
