{{ config(
    schema = 'mart',
    materialized = "table"
) }}

WITH date_dimension AS (

    SELECT
        *
    FROM
        {{ ref('stg_dates') }}
)
SELECT
    dd.*
FROM
    date_dimension dd
