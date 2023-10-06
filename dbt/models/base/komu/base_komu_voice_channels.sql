{{ config (
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'status' AS status,
        _airbyte_data :: json -> 'newRoomName' AS newRoomName,
        _airbyte_data :: json -> 'originalName' AS originalName,
        _airbyte_data :: json -> 'createdTimestamp' AS createdTimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_voicechannels'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('status') }},
        {{ trim_qoutes_col('newRoomName') }},
        {{ trim_qoutes_col('originalName') }},
        {{ trim_qoutes_col('createdTimestamp') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS komu_voice_channel_id,
        status :: VARCHAR,
        newRoomName :: VARCHAR AS new_room_name,
        originalName :: VARCHAR AS original_name,
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
