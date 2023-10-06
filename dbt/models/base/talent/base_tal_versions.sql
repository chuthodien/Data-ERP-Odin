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
            'tal_versions'
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_version_id",
        tenantid :: INT AS tenant_id,
        employeeid :: INT AS employee_id,
        languageid :: INT AS language_id,
        positionid :: INT AS position_id,
        versionname :: VARCHAR AS version_name,
        isdeleted :: BOOLEAN AS is_deleted,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
        lastmodifieruserid :: INT AS last_modifier_user_id,
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
