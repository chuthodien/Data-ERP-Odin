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
            'tal_positionsettings'
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_position_setting_id",
        imsinfo :: VARCHAR AS ims_info,
        tenantid :: INT AS tenant_id,
        usertype :: INT AS user_type,
        isdeleted :: BOOLEAN AS is_deleted,
        discordinfo :: VARCHAR AS discord_info,
        "lmscourseid" :: VARCHAR AS ims_course_id,
        projectinfo :: VARCHAR AS project_info,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
        "lmscoursename" :: VARCHAR AS ims_course_name,
        subpositionid :: INT AS sub_position_id,
        lastmodifieruserid :: INT AS last_modifier_user_id,
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
