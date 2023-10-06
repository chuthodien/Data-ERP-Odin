{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (
    SELECT
	    *
    FROM
        {{ source(
            'raw_finfast',
            'fin_workflowstatustransitionpermissions'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "Id" :: INT AS "workflow_status_transition_permission_id",
        "roleid" :: INT AS "role_id",
        "tenantid" :: INT AS "tenant_id",	
        "isdeleted" :: BOOLEAN AS "is_deleted",
        {{ to_timestamp("creationtime") }} :: TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }} :: TIMESTAMP AS "deletion_time",
        "transitionid" :: INT AS "transition_id",
        "creatoruserid" :: INT AS "creatoruser_id",
        "deleteruserid" :: INT AS "deleteruser_id",
        "lastmodifieruserid" :: INT AS "lastmodifieruser_id",
        {{ to_timestamp("lastmodificationtime") }} :: TIMESTAMP AS "last_modification_time"
    FROM
	    source
)
SELECT
    *
FROM
    data_type_rename_conversion

