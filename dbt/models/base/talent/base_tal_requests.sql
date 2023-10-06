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
            "tal_requests"
        ) }}
),
data_type_rename_conversion AS(
    SELECT
        "Id" :: INT AS "tal_request_id",
        "Level" :: INT AS "level",
        "status" :: INT,
        "note",
        "priority" :: INT,
        "quantity" :: INT,
        branchid :: INT AS branch_id,
        tenantid :: INT AS tenant_id,
        isdeleted :: BOOLEAN AS is_deleted,
        subpositionid :: INT AS sub_position_id,
        usertype :: INT AS user_type,
        {{ to_timestamp("timeneed") }} :: TIMESTAMP AS time_need,
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS creation_time,
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS deletion_time,
        creatoruserid :: INT AS creator_user_id,
        deleteruserid :: INT AS deleter_user_id,
        lastmodifieruserid :: INT AS last_modifier_user_id,
        lastmodificationtime :: TIMESTAMP AS last_modification_time,
        projecttoolrequestid :: INT AS project_tool_request_id
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
