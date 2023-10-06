{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref( 'base_crm_clients' ) }}

), 
data_selection AS (
    SELECT
        *
    FROM
        source
),
customer_status AS (
    SELECT
        *      
    FROM
        {{ ref( 'customer_status' ) }}
),
customer_type AS (
    SELECT
        *      
    FROM
        {{ ref( 'customer_type' ) }}
),
data_type_rename_conversion AS (
    SELECT
        "crm_client_id",
        "email",
        "name",
        "client_type",
        "phone",
        "client_status",
        "country",
        "website",
        "crm_region_id",
        "description",
        "is_deleted",
        {{add_meta_columns('customer_')}}        
    FROM
        data_selection
) 
SELECT
    s.*,
    cs.*,
    ct.*
FROM
    data_type_rename_conversion s
LEFT JOIN customer_status cs
ON cs.customer_status_id = s.client_status
LEFT JOIN customer_type ct
ON ct.customer_type_id = s.client_type


