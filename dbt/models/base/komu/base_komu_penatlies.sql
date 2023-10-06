{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'id' as id,
        _airbyte_data::json->'delete' as delete,
        _airbyte_data::json->'reason' as reason,
        _airbyte_data::json->'userId' as userid,
        _airbyte_data::json->'ammount' as ammount,
        _airbyte_data::json->'isReject' as isreject,
        _airbyte_data::json->'username' as username,
        _airbyte_data::json->'channelId' as channelid,
        _airbyte_data::json->'createdTimestamp' as createdtimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_penatly'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('id')}},
        {{trim_qoutes_col('delete')}},
        {{trim_qoutes_col('reason')}},
        {{trim_qoutes_col('userid')}},
        {{trim_qoutes_col('ammount')}},
        {{trim_qoutes_col('isreject')}},
        {{trim_qoutes_col('username')}},
        {{trim_qoutes_col('channelid')}},
        {{trim_qoutes_col('createdtimestamp')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        userid :: VARCHAR AS komu_user_id,
        username :: VARCHAR AS komu_user_name,
        channelid :: VARCHAR AS channel_id,
        reason :: VARCHAR AS penatly_reason,
        ammount :: VARCHAR AS penatly_amount,
        delete :: BOOLEAN as is_deleted,
        isreject :: BOOLEAN as is_rejected,
        TO_TIMESTAMP(createdtimestamp::NUMERIC / 1000)::TIMESTAMP created_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
