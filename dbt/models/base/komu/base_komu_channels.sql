{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'name' AS NAME,
        _airbyte_data :: json -> 'nsfw' AS nsfw,
        _airbyte_data :: json -> 'type' AS TYPE,
        _airbyte_data :: json -> 'rawPosition' AS raw_position,
        _airbyte_data :: json -> 'lastMessageId' AS last_message_id,
        _airbyte_data :: json -> 'rateLimitPerUser' AS rate_limit_per_user
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_channel'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('name') }},
        {{ trim_qoutes_col('nsfw') }},
        {{ trim_qoutes_col('type') }},
        {{ trim_qoutes_col('raw_position') }},
        {{ trim_qoutes_col('last_message_id') }},
        {{ trim_qoutes_col('rate_limit_per_user') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        id :: VARCHAR AS komu_channel_id,
        NAME :: VARCHAR AS channel_name,
        COALESCE(
            nsfw :: BOOLEAN,
            FALSE
        ) :: BOOLEAN AS nsfw,
        TYPE :: VARCHAR AS channel_type,
        raw_position :: VARCHAR AS raw_position,
        last_message_id :: VARCHAR,
        rate_limit_per_user :: INT
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
