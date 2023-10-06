{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_chat_channel AS (
    SELECT
        *
    FROM
        {{ ref('dim_chat_channel') }}
),
dim_chat_message AS (
    SELECT
        *
    FROM
        {{ ref('dim_chat_message') }}
),
data_aggrated AS (
    SELECT 
        dcm.date_at,
        dcm.employee_email,
        dcc2.komu_channel_id,
        dcc2.channel_name,
        COUNT(DISTINCT dcm.komu_msg_id) AS total_messages
    FROM
        dim_chat_message dcm 
    LEFT JOIN dim_chat_channel dcc
        ON dcc.komu_channel_id = dcm.komu_channel_id
    LEFT JOIN dim_chat_channel dcc2
        ON dcc2.komu_channel_id = dcc.komu_channel_id
    GROUP BY dcm.komu_msg_id, dcm.employee_email, dcm.date_at, dcc2.komu_channel_id, dcc2.channel_name
)
SELECT
    *
FROM
    data_aggrated dt
