{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_employee_absence AS (
    SELECT
        *
    FROM
        {{ ref('stg_employee_absence') }}
),
absence_types AS (
    SELECT
        *
    FROM {{ ref('absence_types')}}
),
absence_status AS (
    SELECT
        *
    FROM {{ref('absence_status')}}
)
SELECT
    *
FROM
    stg_employee_absence sea
LEFT JOIN absence_types abt
    ON sea.absence_type = abt.absence_type_id
LEFT JOIN absence_status abst
    ON sea.absence_status = abst.absence_status_id