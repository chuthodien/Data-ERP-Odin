{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'raw_project',
            'prj_branchs'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" AS prj_branch_id,
        code AS branch_code,
        "Name" AS branch_name,
        color,
        isdeleted AS is_deleted,
        displayname AS display_name,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        creatoruserid :: INT AS creatoruser_id,
        deleteruserid :: INT AS deleteruser_id,
        lastmodifieruserid :: INT AS lastmodifieruser_id,
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
