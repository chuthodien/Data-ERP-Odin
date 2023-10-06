{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref('base_tal_subpositions') }}
    WHERE
        is_deleted IS FALSE
),
data_type_rename_conversion AS(
    SELECT
        tal_sub_position_id AS sub_position_id,
        tal_sub_position_name AS sub_position_name,
        colorcode,
        tal_position_id AS position_id,
        tenant_id,
        creation_time,
        deletion_time,
        last_modification_time
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
