{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_tal_abpusers'
        ) }}
),  

data_mapped AS (
    SELECT
        *
    FROM
        source
)
SELECT
    *
FROM
    data_mapped 
