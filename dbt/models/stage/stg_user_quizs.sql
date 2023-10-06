{{ config(
    materialized = 'table',
    schema = 'stage'
) }}
WITH source AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_komu_user_quizs'
        ) }}
    WHERE quiz_id IS NOT NULL
)
SELECT
    *
FROM 
    source
