{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH tsh_external_customers AS(

    SELECT
        customer_id
    FROM
        {{ ref('stg_tsh_customers') }}
    WHERE
        NAME NOT LIKE 'NCC%'
),
tsh_projects AS(
    SELECT
        *
    FROM
        {{ ref('stg_tsh_projects') }}
),
data_mapped AS(
    SELECT
        *
    FROM
        tsh_projects AS prj
        INNER JOIN tsh_external_customers AS ec
        ON prj.tsh_customer_id = ec.customer_id
)
SELECT
    *
FROM
    data_mapped
