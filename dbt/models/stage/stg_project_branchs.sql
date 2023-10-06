{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_prj_branchs') }}
    WHERE
        is_deleted IS FALSE
),
data_selection AS (
    SELECT
        prj_branch_id,
        branch_code,
        branch_name,
        color,
        display_name
    FROM
        source
)
SELECT
    *
FROM
    data_selection
