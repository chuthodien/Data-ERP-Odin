{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'task' AS task,
        _airbyte_data :: json -> 'cancel' AS cancel,
        _airbyte_data :: json -> 'repeat' AS REPEAT,
        _airbyte_data :: json -> 'reminder' AS reminder,
        _airbyte_data :: json -> 'channelId' AS channelId,
        _airbyte_data :: json -> 'createdTimestamp' AS createdTimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_meeting'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('task') }},
        {{ trim_qoutes_col('cancel') }},
        {{ trim_qoutes_col('repeat') }},
        {{ trim_qoutes_col('reminder') }},
        {{ trim_qoutes_col('channelId') }},
        {{ trim_qoutes_col('createdTimestamp') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS komu_meeting_id,
        task :: VARCHAR,
        cancel :: BOOLEAN AS is_cancelled,
        REPEAT :: VARCHAR,
        reminder :: BOOLEAN,
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
