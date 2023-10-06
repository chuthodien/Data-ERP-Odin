{{ config(
    materialized = 'table',
    schema = 'mart',
    indexes=[
      {'columns': ['employee_email'], 'type': 'hash'},
    ]
) }}

WITH stg_chat_messages AS (
    SELECT
        *
    FROM
        {{ ref( 'stg_chat_messages' ) }}
    WHERE employee_email = 'KOMU@ncc.asia'
),
dim_chat_channel AS (
    SELECT
        *
    FROM 
        {{ ref('dim_chat_channel')}}
)
SELECT
    scm.*,
    dcc.channel_name
FROM
    stg_chat_messages scm
LEFT JOIN dim_chat_channel dcc
    ON dcc.komu_channel_id = scm.komu_channel_id
