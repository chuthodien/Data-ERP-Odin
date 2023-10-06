{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (
    SELECT 
        *
    FROM
        {{source(
            'contest_result_sheet',
            'contest_result'
        )}}
),
data_type_rename_conversion AS (
    SELECT
        "email" :: VARCHAR AS "employee_email",
        "point" :: FLOAT AS "contest_point"
    FROM 
        source
)
SELECT
    *
FROM 
    data_type_rename_conversion