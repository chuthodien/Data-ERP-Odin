{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'Id' as id,
        _airbyte_data::json->'Mail' as mail,
        _airbyte_data::json->'Name' as name,
        _airbyte_data::json->'Role' as role,
        _airbyte_data::json->'Phone' as phone,
        _airbyte_data::json->'TenantId' as tenantid,
        _airbyte_data::json->'FirstName' as firstname,
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
            '_airbyte_raw_crm_contacts'
        ) }}
),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('mail')}},
        {{trim_qoutes_col('name')}},
        {{trim_qoutes_col('role')}},
        {{trim_qoutes_col('phone')}},
        {{trim_qoutes_col('tenantid')}},
        {{trim_qoutes_col('firstname')}},
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
        id :: INT AS "crm_contact_id",
        mail :: VARCHAR AS "email",
        name :: VARCHAR AS "name",
        role :: VARCHAR AS "role",
        phone :: VARCHAR AS "phone",
        firstname :: VARCHAR AS "firstname",
        description :: VARCHAR AS "description",
        isdeleted::BOOLEAN AS "is_deleted",
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
