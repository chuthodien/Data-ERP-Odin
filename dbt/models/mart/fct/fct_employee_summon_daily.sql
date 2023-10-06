{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH dim_chat_summon AS (
    SELECT
        *
    FROM
        {{ ref('dim_chat_summon') }}
),
data_aggrated AS (
    SELECT 
        dcs.date_at,
        dcs.employee_email,
        COUNT(*) AS total_summon
    FROM
        dim_chat_summon dcs 
    GROUP BY 
        dcs.employee_email, 
        dcs.date_at
)
SELECT
    *
FROM
    data_aggrated dt