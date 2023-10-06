{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_komu_mentioneds'
        ) }}
),
stg_employee AS (
    SELECT
        *
    FROM
        {{ ref('stg_employee') }}
),
data_selection AS (
    SELECT
        *
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        is_noti,
        is_punish,
        is_confirmed,
        author_id,
        channel_id,
        message_id,
        target_id,
        created_time,
        (
            CASE
                WHEN created_time :: TIME > '17:30:00'
                AND created_time :: TIME < '24:00:00' THEN DATE_TRUNC(
                    'day',
                    created_time
                ) + '17:30:00' :: TIME
                WHEN created_time :: TIME < '08:30:00' THEN DATE_TRUNC(
                    'day',
                    created_time
                ) + '17:30:00' :: TIME - INTERVAL '1 day'
                ELSE created_time
            END
        ) AS normalized_created_time,
        reaction_time,
        (
            CASE
                WHEN reaction_time :: TIME > '17:30:00'
                AND reaction_time :: TIME < '24:00:00' THEN DATE_TRUNC(
                    'day',
                    reaction_time
                ) + '08:30:00' :: TIME + INTERVAL '1 day'
                WHEN reaction_time :: TIME < '08:30:00' THEN DATE_TRUNC(
                    'day',
                    reaction_time
                ) + '08:30:00' :: TIME
                ELSE reaction_time
            END
        ) AS normalized_reaction_time,
        {{ add_date_at(
            'created_time',
            'created_date'
        ) }},
        {{ add_date_at(
            'reaction_time',
            'reaction_date'
        ) }},
        {{ add_date_at('created_time') }},
        se.email AS author_email,
        se2.email AS target_email
    FROM
        data_selection ds
        LEFT JOIN stg_employee se
        ON ds.author_id = se.komu_user_id
        LEFT JOIN stg_employee se2
        ON ds.target_id = se2.komu_user_id
)
SELECT
    *
FROM
    data_type_rename_conversion
