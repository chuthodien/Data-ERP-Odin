{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_komu_msgs'
        ) }}
),
stg_employee AS (
    SELECT
        *
    FROM
        {{ ref('base_komu_users') }}
),
data_selection AS (
    SELECT
        *
    FROM
        source
),
data_mapped AS (
    SELECT
        komu_msg_id,
        tts,
        msg_type,
        ds.flags,
        nonce,
        embeds,
        is_pinned,
        ds.is_system,
        content,
        guildId,
        mentions,
        stickers,
        komu_channel_id,
        reactions,
        created_time,
        {{ add_date_at('created_time') }},
        CONCAT(
            se.email,
            '@ncc.asia'
        ) AS employee_email
    FROM
        data_selection ds
        LEFT JOIN stg_employee se
        ON ds.authorId = se.komu_user_id
    WHERE
        se.email IS NOT NULL
)
SELECT
    *
FROM
    data_mapped
