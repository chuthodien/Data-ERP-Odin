{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'status' AS status,
        _airbyte_data :: json -> 'authorId' AS authorId,
        _airbyte_data :: json -> 'messageId' AS messageId,
        _airbyte_data :: json -> 'tx8number' AS tx8number,
        _airbyte_data :: json -> 'createdTimestamp' AS createdTimestamp
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_tx8'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('status') }},
        {{ trim_qoutes_col('authorId') }},
        {{ trim_qoutes_col('messageId') }},
        {{ trim_qoutes_col('tx8number') }},
        {{ trim_qoutes_col('createdTimestamp') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        id :: VARCHAR komu_tx8_id,
        status :: VARCHAR AS tx8_status,
        authorId :: VARCHAR AS author_id,
        messageId :: VARCHAR AS message_id,
        tx8number :: INT AS tx8_number,
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
