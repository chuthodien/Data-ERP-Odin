{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_tsh_projects'
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
