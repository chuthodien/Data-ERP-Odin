{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'email' AS email,
        _airbyte_data :: json -> 'point' AS point,
        _airbyte_data :: json -> 'status' AS status,
        _airbyte_data :: json -> 'userid' AS userid,
        _airbyte_data :: json -> 'channelId' AS channelId,
        _airbyte_data :: json -> 'attachment' AS attachment,
        _airbyte_data :: json -> 'createdTimestamp' AS createdTimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_workout'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('email') }},
        {{ trim_qoutes_col('point') }},
        {{ trim_qoutes_col('status') }},
        {{ trim_qoutes_col('userId') }},
        {{ trim_qoutes_col('channelId') }},
        {{ trim_qoutes_col('attachment') }},
        {{ trim_qoutes_col('createdTimestamp') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        id :: INT AS komu_workout_id,
        userId :: VARCHAR AS user_id,
        email :: VARCHAR user_email,
        point :: INT,
        status :: VARCHAR,
        channelId :: VARCHAR AS channel_id,
        attachment :: BOOLEAN,
        TO_TIMESTAMP(
            createdTimestamp :: numeric / 1000
        ) :: TIMESTAMP creation_time
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
