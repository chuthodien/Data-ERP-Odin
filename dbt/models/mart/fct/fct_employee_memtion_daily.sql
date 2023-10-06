{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_chat_mention AS (
    SELECT
        *
    FROM
        {{ ref('dim_chat_mention') }}
),
data_aggrated AS (
    SELECT 
        dcm.date_at,
        dcm.employee_email,
        COUNT(*) AS total_mention
    FROM
        dim_chat_mention dcm 
    GROUP BY 
        dcm.employee_email, 
        dcm.date_at
)
SELECT
    *
FROM
    data_aggrated dt