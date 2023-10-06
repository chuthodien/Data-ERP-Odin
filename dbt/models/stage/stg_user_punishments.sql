{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm2_punishmentemployees'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        hrm_punishmentemployees_id AS punishment_employee_id,
        note,
        money,
        is_deleted,
        employeepid_id,
        creation_time,
        deletion_time,
        last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
