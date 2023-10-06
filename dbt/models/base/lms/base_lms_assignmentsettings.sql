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
            'lms_assignmentsettings'
        ) }}
),

data_type_rename_conversion AS (
    SELECT
        "id" :: VARCHAR AS "lms_assignment_setting_id",
        "point" :: INT AS "point",
        "tenantid" :: INT AS "tenant_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("endtimeutc") }} :: TIMESTAMP AS "end_time_utc",
        "assingmentid" :: VARCHAR AS "assingmentid",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletiontime",
        {{ to_timestamp("starttimeutc") }} :: TIMESTAMP AS "start_time_utc",
        "creatoruserid" :: INT AS "creator_user_id",
        "deleteruserid" :: INT AS "deleter_user_id",
        "numberofduedays" :: INT AS "number_of_due_days",
        "courseinstanceid" :: VARCHAR AS "course_instance_id",
        "lastmodifieruserid" :: INT AS "last_modifier_user_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)

SELECT
    *
FROM
    data_type_rename_conversion
