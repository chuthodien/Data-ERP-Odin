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
            "tal_cvskills"
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_cv_skill_id",
        cvid :: INT AS cv_id,
        note :: VARCHAR,
        "Level" :: INT AS "level",
        skillid :: INT AS skill_id,
        tenantid :: INT AS tenant_id,
        isdeleted :: BOOLEAN AS is_deleted,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
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
