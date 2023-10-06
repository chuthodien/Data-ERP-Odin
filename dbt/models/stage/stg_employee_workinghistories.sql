{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_hrm2_employeeworkinghistories') }}
    WHERE
        is_deleted IS FALSE
)
SELECT
    hrm_employeeworkinghistories_id AS working_history_id,
    note,
    date_at,
    status,
    back_date,
    employee_id
FROM
    source
