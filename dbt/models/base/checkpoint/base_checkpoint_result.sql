{{ config(
    materialized = 'view',
    schema = 'base'
) }}

WITH source AS (
    SELECT 
        *
    FROM
        {{source(
            'checkpoint_result',
            'checkpoint_result'
        )}}
),
data_type_rename_conversion AS (
    SELECT
        "useremail" :: VARCHAR AS "employee_email",
        "point" :: INT AS "checkpoint_point"
    FROM 
        source
)
SELECT
    *
FROM 
    data_type_rename_conversion