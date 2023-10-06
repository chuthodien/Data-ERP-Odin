{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_chat_channels AS (
    SELECT
        *
    FROM
        {{ ref('stg_chat_channels') }}
)
SELECT
    *
FROM
    stg_chat_channels
