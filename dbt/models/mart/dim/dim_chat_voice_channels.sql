{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('stg_chat_voice_channels') }}
)
SELECT
    source.*,
    REGEXP_REPLACE(
        source.new_room_name,
        '.*\(([^)]+)\).*',
        '\1'
    ) AS task_name
FROM
    source
