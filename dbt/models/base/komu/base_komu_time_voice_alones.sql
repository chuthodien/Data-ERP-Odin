{{ config (
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'status' AS status,
        _airbyte_data :: json -> 'channelId' AS channelId,
        _airbyte_data :: json -> 'start_time' AS start_time
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_timevoicealone'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('status') }},
        {{ trim_qoutes_col('channelId') }},
        {{ trim_qoutes_col('start_time') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS komu_time_voice_alone_id,
        status :: BOOLEAN,
        channelId :: VARCHAR AS channel_id,
        TO_TIMESTAMP(
            start_time :: numeric / 1000
        ) :: TIMESTAMP AS start_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
