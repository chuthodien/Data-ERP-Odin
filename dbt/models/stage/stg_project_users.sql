{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_prj_abpusers') }}
)
SELECT
    *
FROM
    source
