{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH source AS (
    SELECT  
        *
    FROM 
        {{ref('stg_checkpoint_rank')}}
)
SELECT 
    *
FROM 
    source