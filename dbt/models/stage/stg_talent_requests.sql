{{ config (
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_tal_requests') }}
    WHERE
        is_deleted IS FALSE
),
data_type_rename_conversion AS(
    SELECT
        tal_request_id AS request_id,
        LEVEL,
        status,
        note,
        priority,
        quantity,
        branch_id,
        is_deleted,
        sub_position_id,
        user_type,
        time_need,
        creation_time,
        deletion_time,
        last_modification_time,
        creator_user_id,
        last_modifier_user_id
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
