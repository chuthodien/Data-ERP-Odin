{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_tal_positions'
        ) }}
    WHERE
        is_deleted IS FALSE
),  

data_type_rename_conversion AS (
    SELECT
        tal_position_id as position_id,
        code as position_code,
        name as position_name,
        tenant_id,
        color_code,
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
    data_type_rename_conversion 
