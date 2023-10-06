{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_chat_mentions AS (

    SELECT
        *
    FROM
        {{ ref(
            'stg_chat_mentions'
        ) }}
),
stg_employee AS (
    SELECT
        *
    FROM
        {{ ref('stg_employee') }}
),
stg_chat_channels AS (
    SELECT
        *
    FROM
        {{ ref('stg_chat_channels') }}
)
SELECT
    scm.*,
    se.*,
    (
        CASE
            WHEN reaction_time IS NOT NULL THEN {{ time_diff_ms(
                'created_time',
                'reaction_time'
            ) }} / 1000 / 60
            ELSE -1
        END
    ) :: bigint AS reaction_duration,
    (
        CASE
            WHEN reaction_time IS NOT NULL THEN (
                {{ time_diff_ms(
                    'normalized_created_time',
                    'normalized_reaction_time'
                ) }} - EXTRACT(
                    DAY
                    FROM
                        (DATE_TRUNC('day', normalized_reaction_time) - DATE_TRUNC('day', normalized_created_time))
                ) * 15 * 60 * 60 * 1000
            ) / 1000 / 60
            ELSE -1
        END
    ) :: bigint AS normalized_reaction_duration,
    cc.channel_name,
    cc.channel_type,
    cc.last_message_id,
    se.email AS employee_email
FROM
    stg_chat_mentions scm
    LEFT JOIN stg_employee se
    ON se.email = scm.author_email
    LEFT JOIN stg_chat_channels cc
    ON scm.channel_id = cc.komu_channel_id
WHERE
    created_time <> '1970-01-01 00:00:00.000'
