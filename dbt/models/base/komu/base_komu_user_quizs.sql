{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS(

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'userId' AS userId,
        _airbyte_data :: json -> 'createAt' AS createAt,
        _airbyte_data :: json -> 'correct' AS correct,
        _airbyte_data :: json -> 'answer' AS answer,
        _airbyte_data :: json -> 'updateAt' AS updateAt,
        _airbyte_data :: json -> 'quizId' AS quizId
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_userquiz'
        ) }}
),
data_cleaning AS(
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('userId') }},
        {{ trim_qoutes_col('createAt') }},
        {{ trim_qoutes_col('correct') }},
        {{ trim_qoutes_col('answer') }},
        {{ trim_qoutes_col('updateAt') }},
        {{ trim_qoutes_col('quizId') }}
    FROM
        source
),
data_type_rename_conversion AS(
    SELECT
        id :: INT AS komu_userquiz_id,
        userId :: VARCHAR AS komu_user_id,
        TO_TIMESTAMP(
            createAt :: numeric / 1000
        ) :: TIMESTAMP AS creation_time,
        TO_TIMESTAMP(
            updateAt :: numeric / 1000
        ) :: TIMESTAMP AS update_at,
        correct :: BOOLEAN AS correct,
        answer :: INT AS answer,
        quizId AS quiz_id
    FROM
        data_cleaning
)
SELECT
    DISTINCT *
FROM
    data_type_rename_conversion
