
{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_tsh_dayoffsettings',
        ) }}
)
SELECT
    *
FROM
    source