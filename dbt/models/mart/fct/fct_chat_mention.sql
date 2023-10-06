{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH dim_chat_mentions AS (

    SELECT
        *
    FROM
        {{ ref('dim_chat_mention') }}
),
dim_chat_message AS (
    SELECT
        *
    FROM
        {{ ref('dim_chat_message') }}
),
data_mapped AS (
    SELECT
        cm.*,
        cme.created_time AS last_active_time
    FROM
        dim_chat_mentions cm
        LEFT JOIN dim_chat_message cme
        ON cm.last_message_id = cme.komu_msg_id
    WHERE
        now() - cme.created_time < INTERVAL '30 days'
)
SELECT
    *
FROM
    data_mapped
