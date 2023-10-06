{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'bwlId' AS bwlId,
        _airbyte_data :: json -> 'count' AS COUNT,
        _airbyte_data :: json -> 'emoji' AS emoji,
        _airbyte_data :: json -> 'guildId' AS guildId,
        _airbyte_data :: json -> 'authorId' AS authorId,
        _airbyte_data :: json -> 'channelId' AS channelId,
        _airbyte_data :: json -> 'createdTimestamp' AS createdTimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_bwlreaction'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('bwlId') }},
        {{ trim_qoutes_col('count') }},
        {{ trim_qoutes_col('emoji') }},
        {{ trim_qoutes_col('guildId') }},
        {{ trim_qoutes_col('authorId') }},
        {{ trim_qoutes_col('channelId') }},
        {{ trim_qoutes_col('createdTimestamp') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS komu_bwl_reaction_id,
        bwlId :: VARCHAR AS bwl_id,
        COUNT,
        emoji,
        guildId :: VARCHAR AS guild_id,
        authorId :: VARCHAR AS author_id,
        channelId :: VARCHAR AS channel_id,
        TO_TIMESTAMP(
            createdTimestamp :: numeric / 1000
        ) :: TIMESTAMP AS creation_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
