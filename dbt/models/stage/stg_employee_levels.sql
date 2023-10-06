{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_hrm2_levels') }}
    WHERE
        is_deleted IS FALSE
),
employee_levels AS (
    SELECT
        *
    FROM
        {{ref('employee_levels')}}
),
data_type_rename_conversion AS (
    SELECT
        hrm_levels_id AS level_id,
        code AS level_code,
        "name" AS level_name,
        el.employee_rank_name
    FROM
        source
        LEFT JOIN employee_levels
        el
        ON source.hrm_levels_id = el.employee_level_id
)
SELECT
    *
FROM
    data_type_rename_conversion
