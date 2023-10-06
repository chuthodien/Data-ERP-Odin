{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref( 'base_crm_regions' ) }}

), 
data_selection AS (
    SELECT
        *
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        "crm_region_id",
        "name",
        {{add_meta_columns('region_')}}        
    FROM
        data_selection
) 
SELECT
    *
FROM
    data_type_rename_conversion


