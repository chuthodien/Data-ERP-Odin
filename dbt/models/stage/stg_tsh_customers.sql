{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS(

    SELECT
        *
    FROM
        {{ ref('base_tsh_customers') }}
),
data_type_rename_conversion AS(
    SELECT
        tsh_customers AS customer_id,
        "name" AS "name",
        is_deleted,
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
