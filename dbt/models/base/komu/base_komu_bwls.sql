{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data::json->'link' as link,
        _airbyte_data::json->'guildId' as guildid,
        _airbyte_data::json->'authorId' as authorid,
        _airbyte_data::json->'channelId' as channelid,
        _airbyte_data::json->'messageId' as messageid,
        _airbyte_data::json->'createdTimestamp' as createdtimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_bwl'
        ) }}

),
data_cleaning AS (
    SELECT
        {{trim_qoutes_col('link')}},
        {{trim_qoutes_col('guildid')}},
        {{trim_qoutes_col('authorid')}},
        {{trim_qoutes_col('channelid')}},
        {{trim_qoutes_col('messageid')}},
        {{trim_qoutes_col('createdtimestamp')}}
    FROM source
),
data_type_rename_conversion AS (
    SELECT
        guildid :: VARCHAR bwl_guild_id,
        authorid :: VARCHAR bwl_author_id,
        channelid :: VARCHAR channel_id,
        messageid :: VARCHAR message_id,
        TO_TIMESTAMP(createdtimestamp::NUMERIC / 1000)::TIMESTAMP created_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
