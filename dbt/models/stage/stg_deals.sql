{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref( 'base_crm_deals' ) }}
    WHERE
        is_deleted = false
), 
data_selection AS (
    SELECT
        *
    FROM
        source
),
deal_status AS (
    SELECT
        *      
    FROM
        {{ ref( 'deal_status' ) }}
),
data_type_rename_conversion AS (
    SELECT
        "crm_deal_id",
        "name",
        "note",
        "amount",
        "deal_status",
        "owner_id",
        "crm_client_id",
        "priority",
        "crm_contact_id",
        "win_reason",
        "lose_reason",
        "closing_time",
        "description",
        "start_time",
        "potential_type", 
        "last_follow_time",
        {{add_meta_columns('detal_')}}        
    FROM
        data_selection
) 
SELECT
    s.*,
    ds.*
FROM
    data_type_rename_conversion s
LEFT JOIN deal_status ds
ON ds.deal_status_id = s.deal_status
