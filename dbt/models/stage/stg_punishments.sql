{{ config(
    materialized = 'table',
    schema = 'stage'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ ref(
            'base_hrm2_punishments'
        ) }}
),
data_type_rename_conversion AS (
    SELECT
        hrm_punishments_id AS punishment_id,
        "date",
        "name",
        is_active,
        is_deleted,
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
