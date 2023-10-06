{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'Id' as id,
        _airbyte_data::json->'Name' as name,
        _airbyte_data::json->'Note' as note,
        _airbyte_data::json->'Amount' as amount,
        _airbyte_data::json->'Status' as status,
        _airbyte_data::json->'OwnerId' as ownerid,
        _airbyte_data::json->'ClientId' as clientid,
        _airbyte_data::json->'Priority' as priority,
        _airbyte_data::json->'TenantId' as tenantid,
        _airbyte_data::json->'ContactId' as contactid,
        _airbyte_data::json->'IsDeleted' as isdeleted,
        _airbyte_data::json->'WinReason' as winreason,
        _airbyte_data::json->'LoseReason' as losereason,
        _airbyte_data::json->'ClosingDate' as closingdate,
        _airbyte_data::json->'Description' as description,
        _airbyte_data::json->'CreationTime' as creationtime,
        _airbyte_data::json->'DeletionTime' as deletiontime,
        _airbyte_data::json->'CreatorUserId' as creatoruserid,
        _airbyte_data::json->'DealStartDate' as dealstartdate,
        _airbyte_data::json->'DeleterUserId' as deleteruserid,
        _airbyte_data::json->'PotentialType' as potentialtype,
        _airbyte_data::json->'DealLastFollow' as deallastfollow,
        _airbyte_data::json->'LastModifierUserId' as lastmodifieruserid,
        _airbyte_data::json->'LastModificationTime' as lastmodificationtime
    FROM
        {{ source(
            'raw_crm',
            '_airbyte_raw_crm_deals'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('name')}},
        {{trim_qoutes_col('note')}},
        {{trim_qoutes_col('amount')}},
        {{trim_qoutes_col('status')}},
        {{trim_qoutes_col('ownerid')}},
        {{trim_qoutes_col('clientid')}},
        {{trim_qoutes_col('priority')}},
        {{trim_qoutes_col('tenantid')}},
        {{trim_qoutes_col('contactid')}},
        {{trim_qoutes_col('isdeleted')}},
        {{trim_qoutes_col('winreason')}},
        {{trim_qoutes_col('losereason')}},
        {{trim_qoutes_col('closingdate')}},
        {{trim_qoutes_col('description')}},
        {{trim_qoutes_col('creationtime')}},
        {{trim_qoutes_col('deletiontime')}},
        {{trim_qoutes_col('creatoruserid')}},
        {{trim_qoutes_col('dealstartdate')}},
        {{trim_qoutes_col('deleteruserid')}},
        {{trim_qoutes_col('potentialtype')}},
        {{trim_qoutes_col('deallastfollow')}},
        {{trim_qoutes_col('lastmodifieruserid')}},
        {{trim_qoutes_col('lastmodificationtime')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS "crm_deal_id",
        name :: VARCHAR AS "name",
        note :: VARCHAR AS "note",
        amount :: FLOAT AS "amount",
        status :: INT AS "deal_status",
        ownerid :: INT AS "owner_id",
        clientid :: INT AS "crm_client_id",
        priority :: INT AS "priority",
        contactid :: INT AS "crm_contact_id",
        winreason :: VARCHAR AS "win_reason",
        losereason :: VARCHAR AS "lose_reason",
        {{ to_timestamp("closingdate")}}::TIMESTAMP AS "closing_time",
        description :: VARCHAR AS "description",
        {{ calc_deal_start_date() }}::TIMESTAMP AS "start_time",
        potentialtype :: INT AS "potential_type", 
        {{ to_timestamp("deallastfollow")}}::TIMESTAMP AS "last_follow_time",
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
