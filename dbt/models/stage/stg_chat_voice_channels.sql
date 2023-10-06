{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_komu_voice_channels') }}
)
SELECT
    source.*
FROM
    source
