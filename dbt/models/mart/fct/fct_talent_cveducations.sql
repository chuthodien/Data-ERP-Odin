{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('dim_talent_cveducations') }}
)
SELECT 
    *
FROM
    source
