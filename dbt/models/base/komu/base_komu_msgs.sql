{{ config(
    materialized = 'view',
    schema = 'base'
) }}
WITH source AS (
    SELECT
        _airbyte_data::json->'id' as komu_msg_id,
        _airbyte_data::json->'tts' as tts,
        _airbyte_data::json->'type' as msg_type,
        _airbyte_data::json->'flags' as flags,
        _airbyte_data::json->'nonce' as nonce,
        _airbyte_data::json->'authorId' as authorId,
        _airbyte_data::json->'embeds' as embeds,
        _airbyte_data::json->'pinned' as is_pinned,
        _airbyte_data::json->'system' as is_system,
        _airbyte_data::json->'content' as content,
        _airbyte_data::json->'deleted' as deleted,
        _airbyte_data::json->'guildId' as guildId,
        _airbyte_data::json->'mentions' as mentions,
        _airbyte_data::json->'stickers' as stickers,
        _airbyte_data::json->'channelId' as komu_channel_id,
        _airbyte_data::json->'reactions' as reactions,
        _airbyte_data::json->'createdTimestamp' as createdTimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_msg'
        ) }}
),
data_cleaning AS (
    SELECT 
        {{trim_qoutes_col('komu_msg_id')}},
        {{trim_qoutes_col('tts')}},
        {{trim_qoutes_col('msg_type')}},
        flags,
        {{trim_qoutes_col('nonce')}},
        embeds,
        {{trim_qoutes_col('is_pinned')}},
        {{trim_qoutes_col('is_system')}},
        {{trim_qoutes_col('content')}},
        {{trim_qoutes_col('deleted')}},
        {{trim_qoutes_col('guildId')}},
        {{trim_qoutes_col('authorId')}},
        mentions,
        stickers,
        {{trim_qoutes_col('komu_channel_id')}},
        reactions,
        {{trim_qoutes_col('createdTimestamp')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT 
        komu_msg_id,
        tts::BOOLEAN AS tts,
        msg_type,
        flags,
        nonce,
        embeds,
        is_pinned,
        is_system::BOOLEAN AS is_system,
        content,
        deleted::BOOLEAN AS is_deleted,
        guildId,
        mentions,
        stickers,
        authorId,
        komu_channel_id,
        reactions,
        TO_TIMESTAMP(createdtimestamp::NUMERIC / 1000)::TIMESTAMP created_time
    FROM data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion

