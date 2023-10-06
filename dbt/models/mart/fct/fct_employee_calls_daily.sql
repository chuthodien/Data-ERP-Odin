{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_chat_call AS (
    SELECT
        *
    FROM
        {{ ref('dim_chat_call') }}
),
data_aggrated AS (
    SELECT 
        dcc.date_at,
        dcc.employee_email,
        SUM(dcc.duration) AS total_duration
    FROM
        dim_chat_call dcc 
    GROUP BY dcc.employee_email, dcc.date_at
)
SELECT
    *
FROM
    data_aggrated dt