{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'Id' as id,
        _airbyte_data::json->'DealId' as dealid,
        _airbyte_data::json->'LevelId' as levelid,
        _airbyte_data::json->'SkillId' as skillid,
        _airbyte_data::json->'Quantity' as quantity,
        _airbyte_data::json->'TenantId' as tenantid,
        _airbyte_data::json->'IsDeleted' as isdeleted,
        _airbyte_data::json->'CreationTime' as creationtime,
        _airbyte_data::json->'DeletionTime' as deletiontime,
        _airbyte_data::json->'CreatorUserId' as creatoruserid,
        _airbyte_data::json->'DeleterUserId' as deleteruserid,
        _airbyte_data::json->'LastModifierUserId' as lastmodifieruserid,
        _airbyte_data::json->'LastModificationTime' as lastmodificationtime
    FROM
        {{ source(
            'raw_crm',
            '_airbyte_raw_crm_dealdetails'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('dealid')}},
        {{trim_qoutes_col('levelid')}},
        {{trim_qoutes_col('skillid')}},
        {{trim_qoutes_col('quantity')}},
        {{trim_qoutes_col('tenantid')}},
        {{trim_qoutes_col('isdeleted')}},
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
        id :: INT AS "crm_dealdetail_id",
        dealid :: INT AS "crm_deal_id",
        levelid :: INT AS "crm_level_",
        skillid :: INT AS "crm_skill_id",
        quantity :: INT AS "quantity",
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
