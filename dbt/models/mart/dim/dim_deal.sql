{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

WITH stg_deals AS (
    SELECT
        *,
        count(crm_deal_id) over (PARTITION BY crm_client_id order by detal_creation_time) client_deal_order 
    FROM
        {{ ref('stg_deals') }}
),
stg_regions AS (
    SELECT
        *
    FROM {{ ref('stg_regions')}}
),
stg_customers AS (
    SELECT
        *
    FROM {{ ref('stg_customers')}}
)
SELECT
    sd.*,
    (CASE
            WHEN sd."client_deal_order" = 1 THEN true
            ELSE false
        END)::BOOLEAN AS "is_new",
    sc.email,
    sc.name AS "customer_name",
    sc.phone AS "customer_phone",
    sc.customer_status_code,
    sc.customer_status_name,
    sc.customer_type_code,
    sc.customer_type_name,
    sc.country AS "customer_country",
    sc.website AS "customer_website",
    sr.crm_region_id,
    sr.name AS "region_name"
FROM
    stg_deals sd
LEFT JOIN stg_customers sc
ON sd.crm_client_id = sc.crm_client_id
LEFT JOIN stg_regions sr
ON sc.crm_region_id = sr.crm_region_id
