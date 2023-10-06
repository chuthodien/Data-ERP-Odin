{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (

    SELECT
        _airbyte_data :: json -> 'bot' AS bot,
        _airbyte_data :: json -> 'email' AS email,
        _airbyte_data :: json -> 'flags' AS flags,
        _airbyte_data :: json -> 'roles' AS roles,
        _airbyte_data :: json -> 'avatar' AS avatar,
        _airbyte_data :: json -> 'system' AS system,
        _airbyte_data :: json -> 'userId' AS userId,
        _airbyte_data :: json -> 'botPing' AS botPing,
        _airbyte_data :: json -> 'deactive' AS deactive,
        _airbyte_data :: json -> 'username' AS username,
        _airbyte_data :: json -> 'scores_quiz' AS scores_quiz,
        _airbyte_data :: json -> 'discriminator' AS discriminator,
        _airbyte_data :: json -> 'roles_discord' AS roles_discord,
        _airbyte_data :: json -> 'last_message_id' AS last_message_id,
        _airbyte_data :: json -> 'last_bot_message_id' AS last_bot_message_id,
        _airbyte_data :: json -> 'last_mentioned_message_id' AS last_mentioned_message_id
    FROM
        {{ source(
            'raw_komu',
            '_airbyte_raw_komu_user'
        ) }}
),
data_cleaning AS (
    SELECT
        {{ trim_qoutes_col('bot') }},
        {{ trim_qoutes_col('email') }},
        {{ trim_qoutes_col('flags') }},
        {{ trim_qoutes_col('roles') }},
        {{ trim_qoutes_col('avatar') }},
        {{ trim_qoutes_col('system') }},
        {{ trim_qoutes_col('userId') }},
        {{ trim_qoutes_col('botPing') }},
        {{ trim_qoutes_col('deactive') }},
        {{ trim_qoutes_col('username') }},
        {{ trim_qoutes_col('scores_quiz') }},
        {{ trim_qoutes_col('discriminator') }},
        {{ trim_qoutes_col('roles_discord') }},
        {{ trim_qoutes_col('last_message_id') }},
        {{ trim_qoutes_col('last_bot_message_id') }},
        {{ trim_qoutes_col('last_mentioned_message_id') }}
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        "userid" :: VARCHAR AS "komu_user_id",
        "bot" :: BOOLEAN AS "is_bot",
        "email" :: VARCHAR AS "email",
        "flags" :: VARCHAR AS "flags",
        "roles" :: VARCHAR AS "roles",
        "avatar" :: VARCHAR AS "avatar",
        "system" :: BOOLEAN AS "is_system",
        "deactive" :: BOOLEAN AS "is_deactivated",
        "username" :: VARCHAR AS "username",
        "scores_quiz" :: VARCHAR AS "quiz_score",
        "discriminator" :: VARCHAR AS "discriminator",
        "roles_discord" :: VARCHAR AS "roles_discord",
        "last_message_id" :: VARCHAR AS "last_message_id",
        "last_bot_message_id" :: VARCHAR AS "last_bot_message_id",
        "last_mentioned_message_id" :: VARCHAR AS "last_mentioned_message_id"
    FROM
        data_cleaning
)
SELECT
    *,
    (
        CASE
            WHEN email LIKE '%@ncc.asia' THEN email
            ELSE email || '@ncc.asia'
        END
    ) :: VARCHAR AS full_email
FROM
    data_type_rename_conversion
