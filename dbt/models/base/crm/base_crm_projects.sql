{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'Id' as id,
        _airbyte_data::json->'Code' as code,
        _airbyte_data::json->'Name' as name,
        _airbyte_data::json->'Type' as type,
        _airbyte_data::json->'DealId' as dealid,
        _airbyte_data::json->'Status' as status,
        _airbyte_data::json->'ClientId' as clientid,
        _airbyte_data::json->'TenantId' as tenantid,
        _airbyte_data::json->'IsDeleted' as isdeleted,
        _airbyte_data::json->'Description' as description,
        _airbyte_data::json->'CreationTime' as creationtime,
        _airbyte_data::json->'DeletionTime' as deletiontime,
        _airbyte_data::json->'CreatorUserId' as creatoruserid,
        _airbyte_data::json->'DeleterUserId' as deleteruserid,
        _airbyte_data::json->'LastModifierUserId' as lastmodifieruserid,
        _airbyte_data::json->'LastModificationTime' as lastmodificationtime
    FROM
        {{ source(
            'raw_crm',
            '_airbyte_raw_crm_projects'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('code')}},
        {{trim_qoutes_col('name')}},
        {{trim_qoutes_col('type')}},
        {{trim_qoutes_col('dealid')}},
        {{trim_qoutes_col('status')}},
        {{trim_qoutes_col('clientid')}},
        {{trim_qoutes_col('tenantid')}},
        {{trim_qoutes_col('isdeleted')}},
        {{trim_qoutes_col('description')}},
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
        id :: INT AS "crm_project_id",
        code :: VARCHAR AS "code",
        name :: VARCHAR AS "name",
        type :: INT AS "type",
        dealid :: INT AS "crm_deal_id",
        status :: INT AS "project_status",
        clientid :: INT AS "crm_client_id",
        description :: VARCHAR AS "description",
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
