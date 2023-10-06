{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'noti' AS noti,
        _airbyte_data :: json -> 'punish' AS punish,
        _airbyte_data :: json -> 'confirm' AS confirm,
        _airbyte_data :: json -> 'authorId' AS authorid,
        _airbyte_data :: json -> 'channelId' AS channelid,
        _airbyte_data :: json -> 'messageId' AS messageid,
        _airbyte_data :: json -> 'mentionUserId' AS mentionuserid,
        _airbyte_data :: json -> 'createdTimestamp' AS createdtimestamp,
        _airbyte_data :: json -> 'reactionTimestamp' AS reactiontimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_mentioned'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('noti') }},
        {{ trim_qoutes_col('punish') }},
        {{ trim_qoutes_col('confirm') }},
        {{ trim_qoutes_col('authorid') }},
        {{ trim_qoutes_col('channelid') }},
        {{ trim_qoutes_col('messageid') }},
        {{ trim_qoutes_col('mentionuserid') }},
        {{ trim_qoutes_col('createdtimestamp') }},
        {{ trim_qoutes_col('reactiontimestamp') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        noti :: BOOLEAN AS is_noti,
        punish :: BOOLEAN AS is_punish,
        confirm :: BOOLEAN AS is_confirmed,
        authorid :: VARCHAR AS author_id,
        channelid :: VARCHAR AS channel_id,
        messageid :: VARCHAR AS message_id,
        mentionuserid :: VARCHAR AS target_id,
        TO_TIMESTAMP(
            (
                CASE
                    WHEN createdtimestamp IS NULL THEN 0
                    WHEN createdtimestamp :: VARCHAR(255) = 'null' THEN 0
                    ELSE createdtimestamp :: numeric
                END
            ) / 1000
        ) :: TIMESTAMP created_time,
        TO_TIMESTAMP(
            (
                CASE
                    WHEN reactiontimestamp IS NULL THEN 0
                    WHEN reactiontimestamp :: VARCHAR(255) = 'null' THEN 0
                    ELSE reactiontimestamp :: numeric
                END
            ) / 1000
        ) :: TIMESTAMP reaction_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
