{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH stg_incomingentries AS (
    SELECT
        *
    FROM {{ref('stg_incomingentries')}}
)
SELECT
    *
FROM 
    stg_incomingentries