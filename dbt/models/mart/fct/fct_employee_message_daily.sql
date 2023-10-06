{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_chat_message AS (
    SELECT
        *
    FROM
        {{ ref('dim_chat_message') }}
),
data_aggrated AS (
    SELECT 
        dcm.date_at,
        dcm.employee_email,
        dcm.channel_name,
        COUNT(*) AS total_message
    FROM
        dim_chat_message dcm 
    GROUP BY 
        dcm.employee_email, 
        dcm.date_at, 
        dcm.channel_name
)
SELECT
    *
FROM
    data_aggrated dt