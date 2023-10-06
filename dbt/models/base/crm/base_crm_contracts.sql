{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'Id' as id,
        _airbyte_data::json->'Name' as name,
        _airbyte_data::json->'Type' as type,
        _airbyte_data::json->'Status' as status,
        _airbyte_data::json->'EndTime' as endtime,
        _airbyte_data::json->'Currency' as currency,
        _airbyte_data::json->'TenantId' as tenantid,
        _airbyte_data::json->'IsDeleted' as isdeleted,
        _airbyte_data::json->'ProjectId' as projectid,
        _airbyte_data::json->'StartTime' as starttime,
        _airbyte_data::json->'CreationTime' as creationtime,
        _airbyte_data::json->'DeletionTime' as deletiontime,
        _airbyte_data::json->'ContractValue' as contractvalue,
        _airbyte_data::json->'CreatorUserId' as creatoruserid,
        _airbyte_data::json->'DeleterUserId' as deleteruserid,
        _airbyte_data::json->'LastModifierUserId' as lastmodifieruserid,
        _airbyte_data::json->'LastModificationTime' as lastmodificationtime
    FROM
        {{ source(
            'raw_crm',
            '_airbyte_raw_crm_contracts'
        ) }}
),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('name')}},
        {{trim_qoutes_col('type')}},
        {{trim_qoutes_col('status')}},
        {{trim_qoutes_col('endtime')}},
        {{trim_qoutes_col('currency')}},
        {{trim_qoutes_col('tenantid')}},
        {{trim_qoutes_col('isdeleted')}},
        {{trim_qoutes_col('projectid')}},
        {{trim_qoutes_col('starttime')}},
        {{trim_qoutes_col('creationtime')}},
        {{trim_qoutes_col('deletiontime')}},
        {{trim_qoutes_col('contractvalue')}},
        {{trim_qoutes_col('creatoruserid')}},
        {{trim_qoutes_col('deleteruserid')}},
        {{trim_qoutes_col('lastmodifieruserid')}},
        {{trim_qoutes_col('lastmodificationtime')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS "crm_contract_id",
        name :: VARCHAR AS "name",
        type :: INT AS "contract_type",
        status :: INT AS "contract_status",
        {{ to_timestamp("endtime")}}::TIMESTAMP AS "endtime",
        currency :: VARCHAR AS "currency",
        projectid :: INT AS "crm_project_id",
        {{ to_timestamp("starttime")}}::TIMESTAMP AS "starttime",
        isdeleted :: BOOLEAN AS "is_deleted",
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
