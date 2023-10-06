{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_tal_branches'
        ) }}
),  

data_mapped AS (
    SELECT
        tal_branch_id as branch_id,
        name as branch_name,
        address as branch_address,
        tenant_id,
        color_code,
        is_deleted,
        display_name as branch_display_name,
        creation_time,
        deletion_time,
        creator_user_id,
        deleter_user_id,
        last_modifier_user_id,
        last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_mapped 
