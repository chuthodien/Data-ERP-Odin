{{ config(
    materialized = 'table',
    schema = 'mart'
) }}
WITH stg_outcomingentries AS (
    SELECT
        *
    FROM {{ref('stg_outcomingentries')}}
)
SELECT
    *
FROM
    stg_outcomingentries