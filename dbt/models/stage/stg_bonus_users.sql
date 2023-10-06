{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm2_bonusemployees'
        ) }}
),
data_mapped AS (
    SELECT
        hrm_bonusemployees_id AS bonus_employee_id,
        money,
        employee_id,
        bonus_id,
        is_deleted,
        note,
        creation_time,
        deletion_time,
        last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_mapped
