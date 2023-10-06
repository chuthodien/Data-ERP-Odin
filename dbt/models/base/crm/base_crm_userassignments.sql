{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'Id' as id,
        _airbyte_data::json->'UserId' as userid,
        _airbyte_data::json->'TenantId' as tenantid,
        _airbyte_data::json->'IsDeleted' as isdeleted,
        _airbyte_data::json->'AssignmentId' as assignmentid,
        _airbyte_data::json->'CreationTime' as creationtime,
        _airbyte_data::json->'DeletionTime' as deletiontime,
        _airbyte_data::json->'CreatorUserId' as creatoruserid,
        _airbyte_data::json->'DeleterUserId' as deleteruserid,
        _airbyte_data::json->'LastModifierUserId' as lastmodifieruserid,
        _airbyte_data::json->'LastModificationTime' as lastmodificationtime
    FROM
        {{ source(
            'raw_crm',
            '_airbyte_raw_crm_userassignments'
        ) }}
),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('userid')}},
        {{trim_qoutes_col('tenantid')}},
        {{trim_qoutes_col('isdeleted')}},
        {{trim_qoutes_col('assignmentid')}},
        {{trim_qoutes_col('creationtime')}},
        {{trim_qoutes_col('deletiontime')}},
        {{trim_qoutes_col('creatoruserid')}},
        {{trim_qoutes_col('deleteruserid')}},
        {{trim_qoutes_col('lastmodifieruserid')}},
        {{trim_qoutes_col('lastmodificationtime')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS "crm_user_assignment_id",
        userid :: INT "crm_user_id",
        assignmentid :: INT AS "crm_assignment_id",
        isdeleted ::BOOLEAN AS "is_deleted",
        {{ to_timestamp("creationtime")}}::TIMESTAMP AS "creation_time",
        {{ to_timestamp("deletiontime") }}::TIMESTAMP AS "deletion_time",
        {{ to_timestamp("lastmodificationtime") }}::TIMESTAMP AS "last_modification_time"
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
