{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_tal_emailtemplates') }}
)
SELECT
    *
FROM
    source
