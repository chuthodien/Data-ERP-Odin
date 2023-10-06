{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm2_bonuses'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        hrm_bonuses_id AS bonus_id,
        "name",
        is_active,
        is_deleted,
        apply_month,
        "creation_time",
        "deletion_time",
        "last_modification_time"
    FROM
        source
)
SELECT
    *
FROM
    data_type_rename_conversion
