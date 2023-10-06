{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_chat_mentions AS (
    SELECT
        *
    FROM
        {{ ref( 'stg_chat_mentions' ) }}
),
stg_employee AS (
    SELECT
        *
    FROM 
        {{ ref('stg_employee')}}
)
SELECT
    scm.*,
    se.*,
    se.email AS employee_email
FROM
    stg_chat_mentions scm
LEFT JOIN stg_employee se
ON se.email = scm.target_email
