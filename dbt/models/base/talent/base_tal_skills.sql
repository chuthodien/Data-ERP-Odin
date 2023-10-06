{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS(

    SELECT
        *
    FROM
        {{ source(
            "raw_talent",
            "tal_skills"
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_skill_id",
        "Name" :: VARCHAR AS "name",
        tenantid :: INT AS tenant_id,
        isdeleted :: BOOLEAN AS is_deleted,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        groupskillid :: INT AS group_skill_id,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
        lastmodifieruserid :: INT AS last_modifier_user_id,
        lastmodificationtime :: TIMESTAMP AS last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
