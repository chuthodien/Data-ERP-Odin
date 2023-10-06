{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS(

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'menu' AS menu,
        _airbyte_data :: json -> 'userId' AS userId,
        _airbyte_data :: json -> 'isCancel' AS isCancel,
        _airbyte_data :: json -> 'channelId' AS channelId,
        _airbyte_data :: json -> 'createdTimestamp' AS createdTimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_order'
        ) }}
),
data_cleaning AS(
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('menu') }},
        {{ trim_qoutes_col('userId') }},
        {{ trim_qoutes_col('isCancel') }},
        {{ trim_qoutes_col('channelId') }},
        {{ trim_qoutes_col('createdTimestamp') }}
    FROM
        source
),
data_type_rename_conversion AS(
    SELECT
        'id',
        menu,
        userId :: VARCHAR AS komu_user_id,
        isCancel :: BOOLEAN AS is_cancelled,
        channelId :: VARCHAR AS komu_channel_id,
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
