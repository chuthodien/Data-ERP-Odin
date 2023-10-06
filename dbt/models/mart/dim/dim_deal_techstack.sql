{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_deal_techstacks AS (
    SELECT
        *
    FROM
        {{ ref('stg_deal_techstacks') }}
),
dim_deal AS (
    SELECT
        *
    FROM {{ ref('dim_deal')}}
)
SELECT

    dd.*,
    sdt.name AS "techstack_name",
    "quantity"
FROM
    stg_deal_techstacks sdt
LEFT JOIN dim_deal dd
ON sdt.crm_deal_id = dd.crm_deal_id