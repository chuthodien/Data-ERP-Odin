{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH source AS(
    SELECT
        *
    FROM
        {{ ref('dim_talent_cvsources') }}
)
SELECT
    *
FROM
    source
