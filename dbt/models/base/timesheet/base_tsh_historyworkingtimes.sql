{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_timesheet',
            'tsh_mytimesheets'
        ) }}
    WHERE
        userid IS NOT NULL
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "tsh_mytimesheet_id",
        {{ to_timestamp("dateat") }} :: TIMESTAMP AS "date_at",
        "userid" :: INT AS "tsh_user_id",
        "note" :: VARCHAR AS "note",
        "status" :: INT AS "status",
        "ischarged" :: BOOLEAN AS "is_charge",
        "typeofwork" :: INT AS "type_of_work",
        {{ minute_to_ms('workingtime') }} :: INT AS "working_time",
        "projecttaskid" :: INT AS "tsh_projecttask_id",
        "projecttargetuserid" :: INT AS "tsh_projecttargetuser_id",
        "isunlockedbyemployee" :: INT AS "is_unlocked_by_employee",
        "targetuserworkingtime" :: INT AS "tsh_targetuserworkingtime_id",
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp('creationtime') }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp('deletiontime') }} :: TIMESTAMP AS "deletion_time",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
