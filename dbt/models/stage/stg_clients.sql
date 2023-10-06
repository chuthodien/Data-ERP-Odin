{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref( 'base_prj_clients' ) }}

), data_selection AS (
    SELECT
        *
    FROM
        source
),
data_type_rename_conversion AS (
    SELECT
        "prj_clients_id",
        "client_code",
        "client_name",
        "client_address",
        {{add_meta_columns('client_')}}        
    FROM
        data_selection
)
SELECT
    *
FROM
    data_type_rename_conversion
