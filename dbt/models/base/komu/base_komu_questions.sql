{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'id' AS id,
        _airbyte_data :: json -> 'role' AS role,
        _airbyte_data :: json -> 'title' AS title,
        _airbyte_data :: json -> 'accept' AS accept,
        _airbyte_data :: json -> 'correct' AS correct,
        _airbyte_data :: json -> 'options' AS options,
        _airbyte_data :: json -> 'isVerify' AS isVerify,
        _airbyte_data :: json -> 'author_email' AS author_email
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_question'
        ) }}
),
data_cleaning AS(
    SELECT
        {{ trim_qoutes_col('id') }},
        {{ trim_qoutes_col('role') }},
        {{ trim_qoutes_col('title') }},
        {{ trim_qoutes_col('accept') }},
        {{ trim_qoutes_col('correct') }},
        {{ trim_qoutes_col('options') }},
        {{ trim_qoutes_col('isVerify') }},
        {{ trim_qoutes_col('author_email') }}
    FROM
        source
),
data_type_rename_conversion AS(
    SELECT
        id :: INT AS komu_question_id,
        role :: VARCHAR AS komu_question_role,
        title :: VARCHAR,
        accept :: BOOLEAN,
        correct :: INT,
        options,
        isVerify :: BOOLEAN AS is_verified,
        author_email
    FROM
        data_cleaning
)
SELECT
    *
FROM
    data_type_rename_conversion
