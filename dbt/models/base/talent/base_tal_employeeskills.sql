{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_talent',
            'tal_employeeskills'
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_employee_skill_id",
        "Level" :: VARCHAR AS "level",
        skillid :: INT AS skill_id,
        tenantid :: INT AS tenant_id,
        isdeleted :: BOOLEAN AS is_deleted,
        skillname :: VARCHAR AS skill_name,
        cvemployeeid :: INT AS cv_employee_id,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        groupskillid :: INT AS group_skill_id,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
        experiencemonth :: INT AS experience_month,
        lastmodifieruserid :: INT AS last_modifier_user_id,
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
