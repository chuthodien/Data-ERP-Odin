{{ config(
    materialized = 'table',
    schema = ' mart'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('stg_talent_users') }}
)
SELECT
    *
FROM
    source
