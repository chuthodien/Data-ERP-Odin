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
            'tal_capabilitysettings'
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_capibility_id",
        note :: VARCHAR,
        tenantid :: INT AS tenant_id,
        usertype :: INT AS user_type,
        isdeleted :: BOOLEAN AS is_deleted,
        capabilityid :: INT AS capability_id,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
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
