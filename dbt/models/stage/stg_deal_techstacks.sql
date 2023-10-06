{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_crm_techstacksdeal'
        ) }}
),
base_crm_techstacks AS (
    SELECT
        *
    FROM
        {{ ref(
            'base_crm_techstacks'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        "crm_techstacksdeal_id",
        "crm_deal_id",
        "crm_techstack_id",
        "quantity",
        {{add_meta_columns('techstacksdeal_')}}        
    FROM
        source
)
SELECT
    s.*,
    bct.name
FROM
    data_type_rename_conversion s
LEFT JOIN base_crm_techstacks bct
ON bct.crm_techstack_id = s.crm_techstack_id